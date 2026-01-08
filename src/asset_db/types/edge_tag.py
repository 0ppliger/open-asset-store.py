from dataclasses import dataclass
from datetime import datetime
from oam import Property
from typing import Optional
from .edge import Edge

@dataclass
class EdgeTag:
    edge:       Edge
    prop:       Property
    id:         Optional[str]      = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @property
    def ttype(self) -> Optional[str]:
        return self.prop.property_type.value
    
    def to_dict(self) -> dict:        
        return {
            "tag_id":     self.id,
            "edge_id":    self.edge.id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "ttype":      self.ttype,
            **self.prop.to_dict()
        }
