from datetime import datetime
from asset_db.types.edge import Edge
from asset_db.types.entity import Entity
from typing import Optional
from typing import cast
from oam import make_oam_object_from_dict
from oam import valid_relationship
from oam import get_relation_by_type
from oam import describe_oam_object
from oam import RelationType
from neo4j import Result
from neo4j.graph import Relationship
from neo4j.time import DateTime

def relationship_to_edge(rel: Relationship, from_entity: Entity, to_entity: Entity) -> Edge:

    eid = rel.element_id

    _created_at = rel.get("created_at")
    if not isinstance(_created_at, DateTime):
        raise Exception("Unable to extract 'created_at'")
    created_at = _created_at.to_native()

    _updated_at = rel.get("updated_at")
    if not isinstance(_updated_at, DateTime):
        raise Exception("Unable to extract 'updated_at'")
    updated_at = _updated_at.to_native()

    _etype = rel.get("etype")
    if _etype is None:
        raise Exception("Unable to extract 'etype'")
    rtype = RelationType(_etype)

    try:
        rel_cls = get_relation_by_type(rtype)
    except Exception as e:
        raise e

    props = describe_oam_object(rel_cls)
    d = {}
    for prop_key in props:
        prop_value = rel.get(prop_key)
        if prop_value is None:
            continue
        
        d[prop_key] = prop_value

    relation = make_oam_object_from_dict(rel_cls, d)

    return Edge(
        id=eid,
        created_at=created_at,
        updated_at=updated_at,
        relation=relation,
        from_entity=from_entity,
        to_entity=to_entity
    )
    
def _create_edge(self, edge: Edge) -> Edge:

    if edge.relation is None \
       or edge.from_entity is None \
       or edge.to_entity is None:
        raise Exception("failed input validation check")

    if edge.from_entity.asset is None \
       or edge.to_entity.asset is None:
        raise Exception("malformed edge")
    
    if not valid_relationship(
            edge.from_entity.asset.asset_type,
            edge.relation.label,
            edge.relation.relation_type,
            edge.to_entity.asset.asset_type
    ):
        raise Exception("{} -{}-> {} is not valid in the taxonomy".format(
            edge.from_entity.asset.asset_type,
            edge.relation.label,
            edge.to_entity.asset.asset_type))

    if not edge.updated_at:
        edge.updated_at = datetime.now()

    dup = self.get_duplicate_edge(edge, edge.updated_at)
    if dup is not None:
        return dup

    if not edge.created_at:
        edge.created_at = datetime.now()

    try:
        record = self.db.execute_query(
            f"""
            MATCH (from:Entity {{entity_id: "{edge.from_entity.id}"}})
            MATCH (to:Entity {{entity_id: "{edge.to_entity.id}"}})
            CREATE (from) -[r:{edge.label} $props]-> (to) RETURN r, from, to
            """,
            {"props": edge.to_dict()},
            result_transformer_=Result.single)
    except Exception as e:
        raise e

    if record is None:
        raise Exception("no records returned from the query")

    r = record.get("r")
    if r is None:
        raise Exception("the record value for the relationship is empty")

    try:
        _edge = relationship_to_edge(r, edge.from_entity, edge.to_entity)
    except Exception as e:
        raise e

    return _edge

def _edge_seen(self, edge: Edge, updated: datetime) -> None:
    try:
        self.db.execute_query(
            f"MATCH ()-[r]->() WHERE elementId(r) = $id SET r.updated_at = localDateTime('{updated.isoformat()}')",
            {"id": edge.id}
        )
    except Exception as e:
        raise e

def _get_duplicate_edge(self, edge: Edge, updated: datetime) -> Optional[Edge]:
    dup = None

    if edge.to_entity is None:
        raise Exception("malformed edge")
    
    try:
        outs = self.outgoing_edges(edge.from_entity)
        for out in outs:
            if edge.to_entity.id == out.to_entity.id and edge.relation == out.relation:
                self.edge_seen(out, updated)

                dup = self.find_edge_by_id(out.id)
                break
    except Exception as e:
        return None

    return dup

def _incoming_edges(self, entity: Entity, since: Optional[datetime] = None, *args: str) -> list[Edge]:
    labels:  list[str]  = list(args)
    results: list[Edge] = []

    query = f"MATCH (:Entity {{entity_id: $id}}) <-[r]- (from:Entity) RETURN r, from.entity_id AS fid"
    if since is not None:
        query = f"MATCH (:Entity {{entity_id: $id}}) <-[r]- (from:Entity) WHERE r.updated_at >= localDateTime('{since.isoformat()}') RETURN r, from.entity_id AS fid"


    try:
        records, summary, keys = self.db.execute_query(query, {
            "id": entity.id
        })
    except Exception as e:
        raise e

    for record in records:
        r = record.get("r")
        if r is None:
            continue

        if len(labels) > 0:
            found = False
            for label in labels:
                if label.casefold() == r.type.casefold():
                    found = True
                    break

            if not found:
                continue

        fid = record.get("fid")
        if fid is None:
            continue

        try:
            from_entity = self.find_entity_by_id(fid)
        except Exception as e:
            raise e
        
        try:
            edge = relationship_to_edge(r, from_entity, entity)
        except Exception as e:
            raise e

        results.append(edge)

    if len(results) == 0:
        raise Exception("no edge found")

    return results

def _outgoing_edges(self, entity: Entity, since: Optional[datetime] = None, *args: str) -> list[Edge]:
    labels:  list[str]  = list(args)
    results: list[Edge] = []

    query = "MATCH (:Entity {entity_id: $id}) -[r]-> (to:Entity) RETURN r, to.entity_id AS tid"
    if since is not None:
        query = f"MATCH (:Entity {{entity_id: $id}}) -[r]-> (to:Entity) WHERE r.updated_at >= localDateTime('{since.isoformat()}') RETURN r, to.entity_id AS tid"

    try:
        records, summary, keys = self.db.execute_query(query, {"id": entity.id})
    except Exception as e:
        raise e

    for record in records:
        r = record.get("r")
        if r is None:
            continue

        if labels:
            found = False
            for label in labels:
                if label.casefold() == r.type.casefold():
                    found = True
                    break

            if not found:
                continue

        tid = record.get("tid")
        if tid is None:
            continue

        try:
            to_entity = self.find_entity_by_id(tid)
        except Exception as e:
            raise e

        try:
            edge = relationship_to_edge(r, entity, to_entity)
        except Exception as e:
            continue

        results.append(edge)

    if not results:
        raise Exception("no edge found")

    return results

def _find_edge_by_id(self, id: str) -> Edge:
    try:
        record = self.db.execute_query(
            f"MATCH (from:Entity) -[r]-> (to:Entity) WHERE elementId(r) = $id RETURN r, from.entity_id as fid, to.entity_id as tid",
            {"id": id},
            result_transformer_=Result.single)
    except Exception as e:
        raise e

    if record is None:
        raise Exception("no edge was found")

    r = record.get("r")
    if r is None:
        raise Exception("the record value for the relationship is empty")

    fid = record.get("fid")
    if fid is None:
        raise Exception("the record value for the from entity ID is empty")

    try:
        from_entity = self.find_entity_by_id(fid)
    except Exception as e:
        raise e
    
    tid = record.get("tid")
    if tid is None:
        raise Exception("the record value for the to entity ID is empty")

    try:
        to_entity = self.find_entity_by_id(tid)
    except Exception as e:
        raise e
    
    try:
        edge = relationship_to_edge(r, from_entity, to_entity)
    except Exception as e:
        raise e

    return edge

def _delete_edge(self, id: str) -> None:
    try:
        self.db.execute_query(
            "MATCH ()-[r]->() WHERE elementId(r) = $id DELETE r",
            {"id": id})
    except Exception as e:
        raise e

