from neo4j import GraphDatabase
from neo4j import Driver
from neo4j import Result
from typing import List
from typing import Optional
from datetime import datetime
from uuid import uuid4
from asset_model import Asset
from asset_model import AssetType
from asset_model import Property
from asset_model import Relation
from asset_model import valid_relationship
from asset_store.types.entity import Entity
from asset_store.types.edge import Edge
from asset_store.types.entity_tag import EntityTag
from asset_store.types.edge_tag import EdgeTag
from asset_store.repository.repository_type import RepositoryType
from asset_store.repository.repository import Repository
from asset_store.repository.neo4j.entity import _create_asset
from asset_store.repository.neo4j.entity import _create_entity
from asset_store.repository.neo4j.entity import _delete_entity
from asset_store.repository.neo4j.entity import _find_entities_by_content
from asset_store.repository.neo4j.entity import _find_entities_by_type
from asset_store.repository.neo4j.entity import _find_entity_by_id
from asset_store.repository.neo4j.edge import _create_edge
from asset_store.repository.neo4j.edge import _create_relation
from asset_store.repository.neo4j.edge import _edge_seen
from asset_store.repository.neo4j.edge import _get_duplicate_edge
from asset_store.repository.neo4j.edge import _find_edge_by_id
from asset_store.repository.neo4j.edge import _incoming_edges
from asset_store.repository.neo4j.edge import _outgoing_edges
from asset_store.repository.neo4j.edge import _delete_edge
from asset_store.repository.neo4j.entity_tag import _create_entity_tag
from asset_store.repository.neo4j.entity_tag import _create_entity_property
from asset_store.repository.neo4j.entity_tag import _delete_entity_tag
from asset_store.repository.neo4j.entity_tag import _find_entity_tags
from asset_store.repository.neo4j.entity_tag import _find_entity_tags_by_content
from asset_store.repository.neo4j.entity_tag import _find_entity_tag_by_id
from asset_store.repository.neo4j.edge_tag import _create_edge_tag
from asset_store.repository.neo4j.edge_tag import _create_edge_property
from asset_store.repository.neo4j.edge_tag import _delete_edge_tag
from asset_store.repository.neo4j.edge_tag import _find_edge_tags
from asset_store.repository.neo4j.edge_tag import _find_edge_tags_by_content
from asset_store.repository.neo4j.edge_tag import _find_edge_tag_by_id

class NeoRepository(Repository):
    db:     Driver
    dbname: str

    def __init__(
            self,
            uri: str,
            auth: tuple[str, str],
            enforce_taxonomy: bool = True
    ):
        self._uri = uri
        self._auth = auth
        # Prefere to not us this. If something lack to the taxonomy
        # please submit a pull request.
        self.enforce_taxonomy = enforce_taxonomy
    
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

    def create_edge(
            self,
            edge: Edge
    ) -> Edge:
        return _create_edge(self, edge)

    def create_relation(
            self,
            relation: Relation,
            from_entity: Entity,
            to_entity: Entity) -> Edge:
        return _create_relation(self, relation, from_entity, to_entity)
    
    def edge_seen(
            self,
            edge: Edge,
            updated: datetime
    ) -> None:
        _edge_seen(self, edge, updated)

    def get_duplicate_edge(
            self, edge:
            Edge,
            updated: datetime
    ) -> Optional[Edge]:
        return _get_duplicate_edge(self, edge, updated)

    def incoming_edges(
            self,
            entity: Entity,
            since: Optional[datetime] = None,
            *args: str
    ) -> List[Edge]:
        return _incoming_edges(self, entity, since, *args)

    def outgoing_edges(
            self,
            entity: Entity,
            since: Optional[datetime] = None,
            *args: str
    ) -> List[Edge]:
        return _outgoing_edges(self, entity, since, *args)

    def find_edge_by_id(
            self,
            id: str
    ) -> Edge:
        return _find_edge_by_id(self, id)

    def delete_edge(
            self,
            id: str
    ) -> None:
        return _delete_edge(self, id)
    
    def create_entity_tag(
            self,
            entity: Entity,
            tag: EntityTag
    ) -> EntityTag:
        return _create_entity_tag(self, entity, tag)

    def find_entity_tag_by_id(
            self,
            id: str
    ) -> EntityTag:
        return _find_entity_tag_by_id(self, id)

    def find_entity_tags(
            self,
            entity: Entity,
            since: Optional[datetime] = None,
            *args: str
    ) -> List[EntityTag]:
        return _find_entity_tags(self, entity, since, *args)

    def create_entity_property(
            self,
            entity: Entity,
            prop: Property
    ) -> EntityTag:
        return _create_entity_property(self, entity, prop)

    def delete_entity_tag(
            self,
            id: str
    ) -> None:
        return _delete_entity_tag(self, id)
    
    def find_entity_tags_by_content(
            self,
            prop: Property,
            since: Optional[datetime] = None
    ) -> List[EntityTag]:
        return _find_entity_tags_by_content(self, prop, since)

    def create_edge_tag(
            self,
            edge: Edge,
            tag: EdgeTag) -> EdgeTag:
        return _create_edge_tag(self, edge, tag)

    def create_edge_property(
            self,
            edge: Edge,
            prop: Property
    ) -> EdgeTag:
        return _create_edge_property(self, edge, prop)

    def find_edge_tag_by_id(
            self,
            id: str
    ) -> EdgeTag:
        return _find_edge_tag_by_id(self, id)

    def find_edge_tags_by_content(
            self,
            prop: Property,
            since: Optional[datetime] = None
    ) -> List[EdgeTag]:
        return _find_edge_tags_by_content(self, prop, since)

    def find_edge_tags(
            self,
            edge: Edge,
            since: Optional[datetime] = None,
            *args: str
    ) -> List[EdgeTag]:
        return _find_edge_tags(self, edge, since, *args)

    def delete_edge_tag(
            self,
            id: str
    ) -> None:
        return _delete_edge_tag(self, id)
