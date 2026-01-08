from neo4j import GraphDatabase
from neo4j import Driver
from neo4j import Result
from typing import List
from typing import Optional
from datetime import datetime
from uuid import uuid4
from oam import Asset
from oam import AssetType
from oam import Property
from oam import valid_relationship
from asset_db.types.entity import Entity
from asset_db.types.edge import Edge
from asset_db.types.entity_tag import EntityTag
from asset_db.types.edge_tag import EdgeTag
from asset_db.repository.repository_type import RepositoryType
from asset_db.repository.repository import Repository
from asset_db.repository.neo4j.extract import node_to_entity_tag
from asset_db.repository.neo4j.extract import node_to_edge_tag
from asset_db.repository.neo4j.extract import relationship_to_edge
from asset_db.repository.neo4j.query import query_node_by_property_key_value
from asset_db.repository.neo4j.entity import _create_asset
from asset_db.repository.neo4j.entity import _create_entity
from asset_db.repository.neo4j.entity import _delete_entity
from asset_db.repository.neo4j.entity import _find_entities_by_content
from asset_db.repository.neo4j.entity import _find_entities_by_type
from asset_db.repository.neo4j.entity import _find_entity_by_id

class NeoRepository(Repository):
    db:     Driver
    dbname: str

    def __init__(self,  uri: str, auth: tuple[str, str]):
        self._uri = uri
        self._auth = auth
    
    def __enter__(self):
        _db = GraphDatabase.driver(self._uri, auth=self._auth)
        _db.verify_connectivity()
        
        self.db = _db
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def get_db_type(self):
        return RepositoryType.Neo4j

    def close(self):
        self.db.close()
        
    def create_entity(
            self,
            entity: Entity
    ) -> Entity:
        return _create_entity(self, entity)
    
    def create_asset(
            self,
            asset: Asset
    ) -> Entity:
        return _create_asset(self, asset)
    
    def find_entity_by_id(
            self,
            id: str
    ) -> Entity:
        return _find_entity_by_id(self, id)
    
    def find_entities_by_content(
            self,
            asset: Asset,
            since: Optional[datetime] = None
    ) -> List[Entity]:
        return _find_entities_by_content(self, asset, since)
    
    def find_entities_by_type(
            self,
            atype: AssetType,
            since: Optional[datetime] = None
    ) -> List[Entity]:
        return _find_entities_by_type(self, atype, since)
    
    def delete_entity(
            self,
            id: str
    ) -> None:
        _delete_entity(self, id)

        
    def edge_seen(self, edge: Edge, updated: datetime) -> None:
        try:
            self.db.execute_query(
                f"MATCH ()-[r]->() WHERE elementId(r) = $id SET r.updated_at = localDateTime('{updated.isoformat()}')",
                {"id": edge.id}
            )
        except Exception as e:
            raise e

    def get_duplicate_edge(self, edge: Edge, updated: datetime) -> Optional[Edge]:
        dup = None

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

    def incoming_edges(self, entity: Entity, since: Optional[datetime] = None, *args: str) -> List[Edge]:
        labels:  List[str]  = list(args)
        results: List[Edge] = []

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
                edge = relationship_to_edge(r)
            except Exception as e:
                raise e

            edge.from_entity = Entity(id=fid)
            edge.to_entity = entity

            results.append(edge)
                
        if len(results) == 0:
            raise Exception("no edge found")

        return results

    def outgoing_edges(self, entity: Entity, since: Optional[datetime] = None, *args: str) -> List[Edge]:
        labels:  List[str]  = list(args)
        results: List[Edge] = []

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
                edge = relationship_to_edge(r)
            except Exception as e:
                continue

            edge.from_entity = entity
            edge.to_entity = Entity(id=tid)
            results.append(edge)

        if not results:
            raise Exception("no edge found")

        return results

    def create_edge(self, edge: Edge) -> Edge:

        if edge.relation == None \
           or edge.from_entity == None \
           or edge.to_entity == None:
            raise Exception("failed input validation check")
        
        if not valid_relationship(
                edge.from_entity.asset.asset_type,
                edge.relation.label,
                edge.relation.relation_type,
                edge.to_entity.asset.asset_type
        ):
            raise Exception("{} -{}-> {} is not valid in the taxonomy").format(
                edge.from_entity.asset_type,
                edge.relation.label,
                edge.from_entity.asset_type)
        
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
            _edge = relationship_to_edge(r)
        except Exception as e:
            raise e

        _edge.from_entity = edge.from_entity
        _edge.to_entity = edge.to_entity

        return _edge

    def find_edge_by_id(self, id: str) -> Edge:
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

        tid = record.get("tid")
        if tid is None:
            raise Exception("the record value for the to entity ID is empty")

        try:
            edge = relationship_to_edge(r)
        except Exception as e:
            raise e

        edge.from_entity = Entity(id=fid)
        edge.to_entity = Entity(id=tid)
        
        return edge

    def delete_edge(self, id: str) -> None:
        try:
            self.db.execute_query(
                "MATCH ()-[r]->() WHERE elementId(r) = $id DELETE r",
                {"id": id})
        except Exception as e:
            raise e

    def find_entity_tags_by_content(self, property: Property, since: Optional[datetime] = None) -> List[EntityTag]:
        tags: List[EntityTag] = []
        qnode = query_node_by_property_key_value("p", "EntityTag", property)

        query = f"MATCH {qnode} RETURN p"
        if since is not None:
            query = f"MATCH {qnode} WHERE p.updated_at >= localDateTime('{since.isoformat()}') RETURN p"

        try:
            records, summary, keys = self.db.execute_query(query, {})
        except Exception as e:
            raise e

        if len(records) == 0:
            raise Exception("no entity tags found")

        for record in records:
            node = record.get("p")
            if node is None:
                continue

            tag = node_to_entity_tag(node)
            if tag:
                tags.append(tag)

        if len(tags) == 0:
            raise Exception("no entity tag found")

        return tags
        
    def create_entity_tag(self, entity: Entity, tag: EntityTag) -> EntityTag:
        existing_tag = None
        if tag.id is not None and tag.id != "":
            existing_tag = EntityTag(
                id=tag.id,
                created_at=tag.created_at,
                updated_at=datetime.now(),
                prop=tag.prop,
                entity=entity,
            )
        else:
            try:
                tags = self.find_entity_tags_by_content(tag.prop)
                for t in tags:
                    if t.entity.id == entity.id:
                        existing_tag = t
                        break

                if existing_tag is not None:
                    existing_tag.entity = entity
                    existing_tag.prop = tag.prop
                    existing_tag.updated_at = datetime.now()
            except Exception as e:
                pass
                        
        if existing_tag is not None:
            if tag.prop.property_type != existing_tag.prop.property_type:
                raise Exception("the property type does not match the existing tag")

            props = existing_tag.to_dict()

            try:
                record = self.db.execute_query(
                    f"MATCH (n:EntityTag {{tag_id: $tid}}) SET n = $props RETURN n",
                    {"tid": existing_tag.id, "props": props},
                    result_transformer_=Result.single
                )
            except Exception as e:
                raise e

            if record is None:
                raise Exception("no records returned from the query")

            node = record.get("n")
            if node is None:
                raise Exception("the record value for the node is nil")

            return node_to_entity_tag(node)

        else:
            if tag.id is None or tag.id == "":
                tag.id = str(uuid4())
            if tag.created_at is None:
                tag.created_at = datetime.now()
            if tag.updated_at is None:
                tag.updated_at = datetime.now()

            tag.entity = entity
            props = tag.to_dict()

            try:
                record = self.db.execute_query(
                    f"CREATE (n:EntityTag:{tag.prop.property_type.value} $props) RETURN n",
                    {"props": props},
                    result_transformer_=Result.single
                )
            except Exception as e:
                raise e

            if record is None:
                raise Exception("no records returned from the query")

            node = record.get("n")
            if node is None:
                raise Exception("the record value for the node is nil")

            return node_to_entity_tag(node)

    def find_entity_tag_by_id(self, id: str) -> EdgeTag:
        try:
            result = self.db.execute_query("MATCH (p:EntityTag {tag_id: $id}) RETURN p", {"id": id})
        except Exception as e:
            raise e

        if result is None:
            raise Exception(f"the entity tag with ID {id} was not found")

        node = result.get("p")
        if node is None:
            raise Exception("the record value for the node is empty")

        return node_to_entity_tag(node)

    def find_entity_tags(self, entity: Entity, since: datetime = None, *args: str) -> List[EntityTag]:
        names = list(args)
        query = f"MATCH (p:EntityTag {{entity_id: '{entity.id}'}}) RETURN p"
        if since is not None:
            query = f"MATCH (p:EntityTag {{entity_id: '{entity.id}'}}) WHERE p.updated_at >= localDateTime('{since.isoformat()}') RETURN p"

        try:
            records, summary, keys = self.db.execute_query(query)
        except Exception as e:
            raise e

        if len(records) == 0:
            raise Exception("no entity tags found")

        tags: List[EntityTag] = []
        for record in records:
            node = record.get("p")
            if node is None:
                continue

            try:
                tag = node_to_entity_tag(node)
            except Exception as e:
                raise e

            if len(names) > 0:
                n = tag.prop.name
                found = n in names
                if not found:
                    continue

            if tag:
                tags.append(tag)
        
                
        if len(tags) == 0:
            raise Exception("no entity tag found")

        return tags

    def create_entity_property(self, entity: Entity, property: Property) -> EntityTag:
        return self.create_entity_tag(entity, EntityTag(prop=property))

    def delete_entity_tag(self, id: str) -> None:
        try:
            self.db.execute_query(
                "MATCH (n:EntityTag {tag_id: $id}) DETACH DELETE n",
                {"id": id})
        except Exception as e:
            raise e
        
    def create_edge_tag(self, edge: Edge, tag: EdgeTag) -> EdgeTag:
        existing_tag = None
        if tag.id is not None and tag.id != "":
            existing_tag = EdgeTag(
                id=tag.id,
                created_at=tag.created_at,
                updated_at=datetime.now(),
                prop=tag.prop,
                edge=edge,
            )
        else:
            try:
                tags = self.find_edge_tags_by_content(tag.prop)
                for t in tags:
                    if t.edge.id == edge.id:
                        existing_tag = t
                        break

                if existing_tag is not None:
                    existing_tag.edge = edge
                    existing_tag.prop = tag.prop
                    existing_tag.updated_at = datetime.now()
            except Exception as e:
                pass
                        
        if existing_tag is not None:
            if tag.prop.property_type != existing_tag.prop.property_type:
                raise Exception("the property type does not match the existing tag")

            props = existing_tag.to_dict()

            try:
                record = self.db.execute_query(
                    f"MATCH (n:EdgeTag {{tag_id: $tid}}) SET n = $props RETURN n",
                    {"tid": existing_tag.id, "props": props},
                    result_transformer_=Result.single
                )
            except Exception as e:
                raise e

            if record is None:
                raise Exception("no records returned from the query")

            node = record.get("n")
            if node is None:
                raise Exception("the record value for the node is nil")

            return node_to_edge_tag(node)

        else:
            if tag.id is None or tag.id == "":
                tag.id = str(uuid4())
            if tag.created_at is None:
                tag.created_at = datetime.now()
            if tag.updated_at is None:
                tag.updated_at = datetime.now()

            tag.edge = edge
            props = tag.to_dict()

            try:
                record = self.db.execute_query(
                    f"CREATE (n:EdgeTag:{tag.prop.property_type.value} $props) RETURN n",
                    {"props": props},
                    result_transformer_=Result.single
                )
            except Exception as e:
                raise e

            if record is None:
                raise Exception("no records returned from the query")

            node = record.get("n")
            if node is None:
                raise Exception("the record value for the node is nil")

            return node_to_edge_tag(node)

    def create_edge_property(self, edge: Edge, property: Property) -> EdgeTag:
        return self.create_edge_tag(edge, EdgeTag(prop=property))

    def find_edge_tag_by_id(self, id: str) -> EdgeTag:
        try:
            result = self.db.execute_query("MATCH (p:EdgeTag {tag_id: $id}) RETURN p", {"id": id})
        except Exception as e:
            raise e

        if result is None:
            raise Exception(f"the edge tag with ID {id} was not found")

        node = result.get("p")
        if node is None:
            raise Exception("the record value for the node is empty")

        return node_to_edge_tag(node)

    def find_edge_tags_by_content(self, prop: Property, since: Optional[datetime] = None) -> List[EdgeTag]:
        tags: List[EdgeTag] = []
        qnode = query_node_by_property_key_value("p", "EdgeTag", prop)

        query = f"MATCH {qnode} RETURN p"
        if since is not None:
            query = f"MATCH {qnode} WHERE p.updated_at >= localDateTime('{since.isoformat()}') RETURN p"
            
        try:
            records, summary, keys = self.db.execute_query(query, {})
        except Exception as e:
            raise e

        if len(records) == 0:
            raise Exception("no edge tags found")

        for record in records:
            node = record.get("p")
            if node is None:
                continue

            tag = node_to_edge_tag(node)
            if tag:
                tags.append(tag)

        if len(tags) == 0:
            raise Exception("no edge tag found")

        return tags

    def find_edge_tags(self, edge: Edge, since: datetime = None, *args: str) -> List[EdgeTag]:
        names = list(args)
        query = f"MATCH (p:EdgeTag {{edge_id: '{edge.id}'}}) RETURN p"
        if since is not None:
            query = f"MATCH (p:EdgeTag {{edge_id: '{edge.id}'}}) WHERE p.updated_at >= localDateTime('{since.isoformat()}') RETURN p"

        try:
            records, summary, keys = self.db.execute_query(query)
        except Exception as e:
            raise e

        if len(records) == 0:
            raise Exception("no edge tags found")

        tags: List[EdgeTag] = []
        for record in records:
            node = record.get("p")
            if node is None:
                continue

            try:
                tag = node_to_edge_tag(node)
            except Exception as e:
                raise e

            if len(names) > 0:
                n = tag.prop.name
                found = n in names
                if not found:
                    continue

            if tag:
                tags.append(tag)
        
                
        if len(tags) == 0:
            raise Exception("no edge tag found")

        return tags

    def delete_edge_tag(self, id: str) -> None:
        try:
            self.db.execute_query(
                "MATCH (n:EdgeTag {tag_id: $id}) DETACH DELETE n",
                {"id": id})
        except Exception as e:
            raise e
