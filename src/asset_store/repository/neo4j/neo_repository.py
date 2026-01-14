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
from asset_store.repository.neo4j.entity import create_asset
from asset_store.repository.neo4j.entity import create_entity
from asset_store.repository.neo4j.entity import delete_entity
from asset_store.repository.neo4j.entity import find_entities_by_content
from asset_store.repository.neo4j.entity import find_entities_by_type
from asset_store.repository.neo4j.entity import find_entity_by_id
from asset_store.repository.neo4j.edge import create_edge
from asset_store.repository.neo4j.edge import create_relation
from asset_store.repository.neo4j.edge import find_edge_by_id
from asset_store.repository.neo4j.edge import incoming_edges
from asset_store.repository.neo4j.edge import outgoing_edges
from asset_store.repository.neo4j.edge import delete_edge
from asset_store.repository.neo4j.entity_tag import create_entity_tag
from asset_store.repository.neo4j.entity_tag import create_entity_property
from asset_store.repository.neo4j.entity_tag import delete_entity_tag
from asset_store.repository.neo4j.entity_tag import find_entity_tags
from asset_store.repository.neo4j.entity_tag import find_entity_tags_by_content
from asset_store.repository.neo4j.entity_tag import find_entity_tag_by_id
from asset_store.repository.neo4j.edge_tag import create_edge_tag
from asset_store.repository.neo4j.edge_tag import create_edge_property
from asset_store.repository.neo4j.edge_tag import delete_edge_tag
from asset_store.repository.neo4j.edge_tag import find_edge_tags
from asset_store.repository.neo4j.edge_tag import find_edge_tags_by_content
from asset_store.repository.neo4j.edge_tag import find_edge_tag_by_id
from asset_store.events import events

class NeoRepository(Repository):
    db:     Driver
    dbname: str

    def __init__(
            self,
            uri: str,
            auth: tuple[str, str],
            enforce_taxonomy: bool = True,
            emit_events: bool = False
    ):
        self._uri = uri
        self._auth = auth
        self._events_buffer = []
        self.enforce_taxonomy = enforce_taxonomy
        self.emit_events = emit_events
    
    def __enter__(self):
        _db = GraphDatabase.driver(self._uri, auth=self._auth)
        _db.verify_connectivity()
        
        self.db = _db
        return self

    def _emit(self, event: events.Event) -> None:
        if self.emit_events:
            self._events_buffer.append(event)

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def get_db_type(self) -> str:
        return RepositoryType.Neo4j

    def close(self) -> None:
        self.db.close()

    def flush_events(self) -> list[events.Event]:
        events = self._events_buffer
        self._events_buffer = []
        return events

    def create_entity(
            self,
            entity: Entity
    ) -> Entity:
        return create_entity(self, entity)
    
    def create_asset(
            self,
            asset: Asset
    ) -> Entity:
        return create_asset(self, asset)
    
    def find_entity_by_id(
            self,
            id: str
    ) -> Entity:
        return find_entity_by_id(self, id)
    
    def find_entities_by_content(
            self,
            asset: Asset,
            since: Optional[datetime] = None
    ) -> List[Entity]:
        return find_entities_by_content(self, asset, since)
    
    def find_entities_by_type(
            self,
            atype: AssetType,
            since: Optional[datetime] = None
    ) -> List[Entity]:
            return find_entities_by_type(self, atype, since)
    
    def delete_entity(
            self,
            id: str
    ) -> Entity:
        return delete_entity(self, id)

    def create_edge(
            self,
            edge: Edge
    ) -> Edge:
        return create_edge(self, edge)

    def create_relation(
            self,
            relation: Relation,
            from_entity: Entity,
            to_entity: Entity
    ) -> Edge:
        return create_relation(self, relation, from_entity, to_entity)

    def incoming_edges(
            self,
            entity: Entity,
            since: Optional[datetime] = None,
            *args: str
    ) -> List[Edge]:
        return incoming_edges(self, entity, since, *args)

    def outgoing_edges(
            self,
            entity: Entity,
            since: Optional[datetime] = None,
            *args: str
    ) -> List[Edge]:
        return outgoing_edges(self, entity, since, *args)

    def find_edge_by_id(
            self,
            id: str
    ) -> Edge:
        return find_edge_by_id(self, id)

    def delete_edge(
            self,
            id: str
    ) -> Edge:
        return delete_edge(self, id)
    
    def create_entity_tag(
            self,
            tag: EntityTag
    ) -> EntityTag:
        return create_entity_tag(self, tag)

    def create_entity_property(
            self,
            entity: Entity,
            prop: Property
    ) -> EntityTag:
        return create_entity_property(self, entity, prop)

    def find_entity_tag_by_id(
            self,
            id: str
    ) -> EntityTag:
        return find_entity_tag_by_id(self, id)

    def find_entity_tags(
            self,
            entity: Entity,
            since: Optional[datetime] = None,
            *args: str
    ) -> List[EntityTag]:
        return find_entity_tags(self, entity, since, *args)

    def delete_entity_tag(
            self,
            id: str
    ) -> EntityTag:
        return delete_entity_tag(self, id)
    
    def find_entity_tags_by_content(
            self,
            prop: Property,
            since: Optional[datetime] = None
    ) -> List[EntityTag]:
        return find_entity_tags_by_content(self, prop, since)

    def create_edge_tag(
            self,
            tag: EdgeTag
    ) -> EdgeTag:
        return create_edge_tag(self, tag)

    def create_edge_property(
            self,
            edge: Edge,
            prop: Property
    ) -> EdgeTag:
        return create_edge_property(self, edge, prop)

    def find_edge_tag_by_id(
            self,
            id: str
    ) -> EdgeTag:
        return find_edge_tag_by_id(self, id)

    def find_edge_tags_by_content(
            self,
            prop: Property,
            since: Optional[datetime] = None
    ) -> List[EdgeTag]:
        return find_edge_tags_by_content(self, prop, since)

    def find_edge_tags(
            self,
            edge: Edge,
            since: Optional[datetime] = None,
            *args: str
    ) -> List[EdgeTag]:
        return find_edge_tags(self, edge, since, *args)

    def delete_edge_tag(
            self,
            id: str
    ) -> EdgeTag:
        return delete_edge_tag(self, id)
