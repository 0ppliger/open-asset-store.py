from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4
from oam import Relation
from .entity import Entity

@dataclass
class Edge:
    id:          uuid4
    created_at:  datetime
    updated_at:  datetime
    relation:    Relation
    from_entity: Entity
    to_entity:   Entity

    @property
    def etype(self) -> str:
        return self.relation.relation_type.value

    @property
    def label(self) -> str:
        return self.relation.label.upper()
    
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
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "etype":      self.etype,
            **_flatten(self.relation.to_dict())
        }

    @staticmethod
    def create(relation: Relation, from_entity: Entity, to_entity: Entity) -> 'Edge':
        return Edge(
	    id=uuid4(),
            created_at=datetime.now(),
            updated_at=datetime.now(),
            relation=relation,
            from_entity=from_entity,
            to_entity=to_entity
        )
