from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4
from oam import Asset

@dataclass
class Entity:
    id:         uuid4
    created_at: datetime
    updated_at: datetime
    asset:      Asset

    @property
    def etype(self) -> str:
        return self.asset.asset_type.value
    
    def to_dict(self) -> dict:
        def _flatten(d) -> dict:
            flat = {}
            for k, v in d.items():
                if isinstance(v, dict):
                    flat.update(_flatten(v))
                else:
                    flat[k] = v
            return flat

        return {
            "entity_id":  str(self.id),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "etype":      self.etype,
            **_flatten(self.asset.to_dict())
        }

    @staticmethod
    def create(asset: Asset) -> 'Entity':
        return Entity(
	    id=uuid4(),
            created_at=datetime.now(),
            updated_at=datetime.now(),
            asset=asset
        )

