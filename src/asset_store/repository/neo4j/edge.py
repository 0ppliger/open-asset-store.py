from datetime import datetime
from turtle import update
from asset_store.events import events
from asset_store.types.edge import Edge
from asset_store.types.entity import Entity
from typing import Optional
from asset_model import OAMObject
from asset_model import valid_relationship
from asset_model import get_relation_by_type
from asset_model import describe_type
from asset_model import Relation
from asset_model import RelationType
from neo4j import Result
from neo4j.graph import Relationship
from neo4j.time import DateTime
from uuid import uuid4

def _relationship_to_edge(rel: Relationship, from_entity: Entity, to_entity: Entity) -> Edge:

    id = rel.get("edge_id")
    if id is None:
        raise Exception("Unable to extract 'edge_id'")

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
    
    props = describe_type(rel_cls)
    d = {}
    for prop_key in props:
        prop_value = rel.get(prop_key)
        if prop_value is None:
            continue
        
        d[prop_key] = prop_value

    extra_props = list(filter(lambda e: e.startswith("extra_"), rel.keys()))
    extra = { key: rel.get(key) for key in extra_props }

    d.update(extra)
        
    relation = OAMObject.from_dict(rel_cls, d)
    
    return Edge(
        id=id,
        created_at=created_at,
        updated_at=updated_at,
        relation=relation,
        from_entity=from_entity,
        to_entity=to_entity
    )

def _find_existing_edge(self, edge: Edge) -> Optional[Edge]:
    if edge.id is not None and edge.id != "":
        return self.find_edge_by_id(edge.id)
    
    try:
        outs = self.outgoing_edges(edge.from_entity)
        for out in outs:
            if edge.to_entity.id == out.to_entity.id and edge.relation.equals(out.relation):                
                return out
    except Exception as e:
        return None

    return None

    
def create_edge(self, edge: Edge) -> Edge:

    if self.enforce_taxonomy and not valid_relationship(
            edge.from_entity.asset.asset_type,
            edge.relation.label,
            edge.relation.relation_type,
            edge.to_entity.asset.asset_type
    ):
        raise Exception("{} -{}-> {} is not valid in the taxonomy".format(
            edge.from_entity.asset.asset_type,
            edge.relation.label,
            edge.to_entity.asset.asset_type))

    new_edge: Optional[Edge] = None
    old_edge: Optional[Edge] = _find_existing_edge(self, edge)

    # If the edge does not exist, create it
    if old_edge is None:
        new_edge = Edge(
            id          = str(uuid4()),
            created_at  = datetime.now(),
            updated_at  = datetime.now(),
            from_entity = edge.from_entity,
            to_entity   = edge.to_entity,
            relation    = edge.relation
        )

        try:
            record = self.db.execute_query(
                f"""
                MATCH (from:Entity {{entity_id: "{new_edge.from_entity.id}"}})
                MATCH (to:Entity {{entity_id: "{new_edge.to_entity.id}"}})
                CREATE (from) -[r:{new_edge.label} $props]-> (to) RETURN r, from, to
                """,
                {"props": new_edge.to_dict()},
                result_transformer_=Result.single)
        except Exception as e:
            raise e

        if record is None:
            raise Exception("no records returned from the query")

        self._emit(events.EdgeInserted(edge=new_edge))
        return new_edge

    # If the edge already exists and has new data, update it
    if edge.relation.is_fresher_than(old_edge.relation):
        new_edge = Edge(
            id          = old_edge.id,
            from_entity = old_edge.from_entity,
            to_entity   = old_edge.to_entity,
            relation    = old_edge.relation.override_with(edge.relation),
            created_at  = old_edge.created_at,
            updated_at  = datetime.now()
        )

        try:
            record = self.db.execute_query(
                f"""
                MATCH (from:Entity {{entity_id: "{new_edge.from_entity.id}"}})
                MATCH (to:Entity {{entity_id: "{new_edge.to_entity.id}"}})
                MATCH (from) -[r:{new_edge.label}]-> (to) 
                SET r = $props
                RETURN r, from, to
                """,
                {"props": new_edge.to_dict()},
                result_transformer_=Result.single)
        except Exception as e:
            raise e

        if record is None:
            raise Exception("no records returned from the query")

        self._emit(events.EdgeUpdated(old_edge=old_edge, edge=new_edge))
        return new_edge

    self._emit(events.EdgeUntouched(edge=old_edge))
    return old_edge

def create_relation(self, relation: Relation, from_entity: Entity, to_entity: Entity) -> Edge:
    return self.create_edge(
        Edge(relation, from_entity, to_entity))

def incoming_edges(self, entity: Entity, since: Optional[datetime] = None, *args: str) -> list[Edge]:
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
            edge = _relationship_to_edge(r, from_entity, entity)
        except Exception as e:
            raise e

        results.append(edge)

    if len(results) == 0:
        return []

    return results

def outgoing_edges(self, entity: Entity, since: Optional[datetime] = None, *args: str) -> list[Edge]:
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
            edge = _relationship_to_edge(r, entity, to_entity)
        except Exception as e:
            continue

        results.append(edge)

    if not results:
        return []

    return results

def find_edge_by_id(self, id: str) -> Edge:
    try:
        record = self.db.execute_query(
            f"MATCH (from:Entity) -[r {{edge_id: $id}}]-> (to:Entity) RETURN r, from.entity_id as fid, to.entity_id as tid",
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
        edge = _relationship_to_edge(r, from_entity, to_entity)
    except Exception as e:
        raise e

    return edge

def delete_edge(self, id: str) -> Edge:
    edge = self.find_edge_by_id(id)

    try:
        self.db.execute_query(
            "MATCH ()-[r {{ edge_id: $id }}]->() DELETE r",
            {"id": id})
    except Exception as e:
        raise e

    self._emit(events.EdgeDeleted(old_edge=edge))
    return edge
