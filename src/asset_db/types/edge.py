from typing import Optional
from dataclasses import dataclass
from datetime import datetime
from oam import Relation
from .entity import Entity

@dataclass
class Edge:
    relation:    Relation
    from_entity: Entity
    to_entity:   Entity
    id:          Optional[str]      = None
    created_at:  Optional[datetime] = None
    updated_at:  Optional[datetime] = None

    @property
    def etype(self) -> str:
        return self.relation.relation_type.value

    @property
    def label(self) -> str:
        return self.relation.label.upper()
    
    def to_dict(self) -> dict:
        return {
            "edge_id":    self.id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "etype":      self.etype,
            **self.relation.to_dict()
        }
