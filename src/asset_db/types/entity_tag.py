from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4
from oam import Property
from .entity import Entity

@dataclass
class EntityTag:
    id:         uuid4
    created_at: datetime
    updated_at: datetime
    entity:     Entity
    property:   Property

    @property
    def ttype(self) -> str:
        return self.property.property_type.value
    
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
            "tag_id":  str(self.id),
            "entity_id":  str(self.entity.id),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "ttype":      self.ttype,
            **_flatten(self.property.to_dict())
        }

    @staticmethod
    def create(entity: Entity, property: Property) -> 'EntityTag':
        return EntityTag(
	    id=uuid4(),
            created_at=datetime.now(),
            updated_at=datetime.now(),
            entity=entity,
            property=property
        )

