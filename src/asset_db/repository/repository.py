from abc import ABC
from abc import abstractmethod
from typing import Optional
from oam import Asset
from oam import AssetType
from oam import Property
from asset_db.types.entity import Entity
from asset_db.types.entity_tag import EntityTag
from asset_db.types.edge import Edge
from asset_db.types.edge_tag import EdgeTag
from datetime import datetime

class Repository(ABC):
    
    @abstractmethod
    def get_db_type(self) -> str:
        pass
    
    @abstractmethod
    def create_entity(self, entity: Entity) -> Entity:
        pass
    
    @abstractmethod
    def create_asset(self, asset: Asset) -> Entity:
        pass
    
    @abstractmethod
    def find_entity_by_id(self, id: str) -> Entity:
        pass
    
    @abstractmethod
    def find_entities_by_content(self, asset: Asset, since: Optional[datetime]) -> list[Entity]:
        pass
    
    @abstractmethod
    def find_entities_by_type(self, atype: AssetType, since: Optional[datetime]) -> list[Entity]:
        pass
    
    @abstractmethod
    def delete_entity(self, id: str) -> None:
        pass
    
    @abstractmethod
    def create_edge(self, edge: Edge) -> Edge:
        pass
    
    @abstractmethod
    def find_edge_by_id(self, id: str) -> Edge:
        pass
    
    @abstractmethod
    def incoming_edges(self, entity: Entity, since: Optional[datetime], *args: str) -> list[Edge]:
        pass
    
    @abstractmethod
    def outgoing_edges(self, entity: Entity, since: Optional[datetime], *args: str) -> list[Edge]:
        pass
    
    @abstractmethod
    def delete_edge(self, id: str) -> None:
        pass
    
    @abstractmethod
    def create_entity_tag(self, entity: Entity, tag: EntityTag) -> EntityTag:
        pass
    
    @abstractmethod
    def create_entity_property(self, entity: Entity, prop: Property) -> EntityTag:
        pass
    
    @abstractmethod
    def find_entity_tag_by_id(self, id: str) -> EntityTag:
        pass
    
    @abstractmethod
    def find_entity_tags_by_content(self, prop: Property, since: Optional[datetime]) -> list[EntityTag]:
        pass
    
    @abstractmethod
    def find_entity_tags(self, entity: Entity, since: Optional[datetime], *args: str) -> list[EntityTag]:
        pass
    
    @abstractmethod
    def delete_entity_tag(self, id: str) -> None:
        pass
    
    @abstractmethod
    def create_edge_tag(self, edge: Edge, tag: EdgeTag) -> EdgeTag:
        pass
    
    @abstractmethod
    def create_edge_property(self, edge: Edge, prop: Property) -> EdgeTag:
        pass
    
    @abstractmethod
    def find_edge_tag_by_id(self, id: str) -> EdgeTag:
        pass
    
    @abstractmethod
    def find_edge_tags_by_content(self, prop: Property, since: Optional[datetime]) -> list[EdgeTag]:
        pass
    
    @abstractmethod
    def find_edge_tags(self, edge: Edge, since: Optional[datetime], *args: str) -> list[EdgeTag]:
        pass
    
    @abstractmethod
    def delete_edge_tag(self, id: str) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

