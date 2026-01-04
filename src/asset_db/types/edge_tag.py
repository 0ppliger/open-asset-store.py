from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4
from oam import Property
from .edge import Edge

@dataclass
class EdgeTag:
    id:         uuid4
    created_at: datetime
    updated_at: datetime
    edge:       Edge
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
            "edge_id":  str(self.edge.id),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "ttype":      self.ttype,
            **_flatten(self.property.to_dict())
        }

    @staticmethod
    def create(edge: Edge, property: Property) -> 'EdgeTag':
        return EdgeTag(
	    id=uuid4(),
            created_at=datetime.now(),
            updated_at=datetime.now(),
            edge=edge,
            property=property
        )

