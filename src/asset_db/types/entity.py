from dataclasses import dataclass
from datetime import datetime
from oam import Asset
from typing import Optional

@dataclass
class Entity:
    asset:      Asset
    id:         Optional[str]      = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @property
    def etype(self) -> Optional[str]:
        return self.asset.asset_type.value
    
    def to_dict(self) -> dict:
        return {
            "entity_id":  self.id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "etype":      self.etype,
            **self.asset.to_dict()
        }

