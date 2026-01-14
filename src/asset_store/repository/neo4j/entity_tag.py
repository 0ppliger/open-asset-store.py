from asset_store.events import events
from asset_store.types import Entity
from asset_store.types import EntityTag
from asset_model import Property
from asset_model import PropertyType
from asset_model import get_property_by_type
from asset_model import describe_type
from asset_model import OAMObject
from typing import Optional
from typing import cast
from datetime import datetime
from uuid import uuid4
from neo4j import Result
from neo4j.graph import Node
from uuid import uuid4

def _node_to_entity_tag(self, node: Node) -> EntityTag:
    id = node.get("tag_id")
    if id is None:
        raise Exception("Unable to extract 'tag_id'")

    entity_id = node.get("entity_id")
    if entity_id is None:
        raise Exception("Unable to extract 'entity_id'")

    try:
        entity = self.find_entity_by_id(entity_id)
    except Exception as e:
        raise e
    
    _created_at = node.get("created_at")
    if _created_at is None:
        raise Exception("Unable to extract 'created_at'")
    created_at = _created_at.to_native()

    _updated_at = node.get("updated_at")
    if _updated_at is None:
        raise Exception("Unable to extract 'created_at'")
    updated_at = _updated_at.to_native()
    
    _ttype = node.get("ttype")
    if _ttype is None:
        raise Exception("Unable to extract 'ttype'")
    property_type = PropertyType(_ttype)

    try:
        property_cls = get_property_by_type(property_type)
    except Exception as e:
        raise e

    props = describe_type(property_cls)
    d = {}
    for prop_key in props:
        prop_value = node.get(prop_key)
        if prop_value is None:
            continue
        
        d[prop_key] = prop_value

    extra_props = list(filter(lambda e: e.startswith("extra_"), node.keys()))
    extra = { key: node.get(key) for key in extra_props }

    d.update(extra)
        
    prop = cast(Property, OAMObject.from_dict(property_cls, d))
        
    return EntityTag(
        id=id,
        entity=entity,
        created_at=created_at,
        updated_at=updated_at,
        prop=prop
    )

def _find_existing_entity_tag(self, tag: EntityTag) -> Optional[EntityTag]:
    if tag.id is not None and tag.id != "":
        return self.find_entity_tag_by_id(tag.id)
    else:
        findings = self.find_entity_tags_by_content(tag.prop)
        if len(findings) > 0:
            return findings[0]
    return None


def create_entity_tag(self, tag: EntityTag) -> EntityTag:

    if tag.prop is None:
        raise Exception("malformed entity tag")

    new_tag: Optional[EntityTag] = None
    old_tag: Optional[EntityTag] = _find_existing_entity_tag(self, tag)
    
    # If the entity tag does not exist, create it
    if old_tag is None:
        new_tag = EntityTag(
            id         = str(uuid4()),
            created_at = datetime.now(),
            updated_at = datetime.now(),
            entity     = tag.entity,
            prop       = tag.prop
        )
    
        props = new_tag.to_dict()

        try:
            record = self.db.execute_query(
                f"CREATE (n:EntityTag:{tag.prop.property_type.value} $props) RETURN n",
                {"props": props},
                result_transformer_=Result.single)
        except Exception as e:
            raise e

        if record is None:
            raise Exception("no records returned from the query")

        self._emit(events.EntityTagInserted(tag=new_tag))
        return new_tag
    
    # If the entity tag already exists and has new data, update it
    if tag.prop.is_fresher_than(old_tag.prop):
        new_tag = EntityTag(
            id         = old_tag.id,
            created_at = old_tag.created_at,
            updated_at = datetime.now(),
            entity     = old_tag.entity,
            prop       = old_tag.prop.override_with(tag.prop)
        )

        props = new_tag.to_dict()

        try:
            record = self.db.execute_query(
                f"MATCH (n:EntityTag {{tag_id: $tid}}) SET n = $props RETURN n",
                {"tid": new_tag.id, "props": props},
                result_transformer_=Result.single
            )
        except Exception as e:
            raise e

        self._emit(events.EntityTagUpdated(old_tag=old_tag, tag=new_tag))
        return new_tag

    # If the entity already exists and has no new data, return the existing entity
    self._emit(events.EntityTagUntouched(tag=old_tag))
    return old_tag


def create_entity_property(self, entity: Entity, prop: Property) -> EntityTag:
    return self.create_entity_tag(EntityTag(entity=entity, prop=prop))

def find_entity_tags_by_content(self, prop: Property, since: Optional[datetime] = None) -> list[EntityTag]:
        tags: list[EntityTag] = []

        props = prop.to_dict()
        props_filters = " AND ".join([f"p.{k} = ${k}" for k in props.keys()])
    
        query = f"MATCH (p:EntityTag:{prop.property_type.value}) WHERE {props_filters} RETURN p"
        if since is not None:
            query = f"MATCH (p:EntityTag:{prop.property_type.value}) WHERE {props_filters} AND p.updated_at >= localDateTime('{since.isoformat()}') RETURN p"
            
        try:
            records, summary, keys = self.db.execute_query(query, props)
        except Exception as e:
            raise e

        for record in records:
            node = record.get("p")
            if node is None:
                continue

            tag = _node_to_entity_tag(self, node)
            if tag:
                tags.append(tag)

        if len(tags) == 0:
            return []

        return tags
        
def find_entity_tag_by_id(self, id: str) -> EntityTag:
    try:
        result = self.db.execute_query("MATCH (p:EntityTag {tag_id: $id}) RETURN p", {"id": id})
    except Exception as e:
        raise e

    if result is None:
        raise Exception(f"the entity tag with ID {id} was not found")

    node = result.get("p")
    if node is None:
        raise Exception("the record value for the node is empty")

    return _node_to_entity_tag(self, node)

def find_entity_tags(self, entity: Entity, since: Optional[datetime] = None, *args: str) -> list[EntityTag]:
    names = list(args)
    query = f"MATCH (p:EntityTag {{entity_id: '{entity.id}'}}) RETURN p"
    if since is not None:
        query = f"MATCH (p:EntityTag {{entity_id: '{entity.id}'}}) WHERE p.updated_at >= localDateTime('{since.isoformat()}') RETURN p"

    try:
        records, summary, keys = self.db.execute_query(query)
    except Exception as e:
        raise e

    if len(records) == 0:
        raise Exception("no entity tags found")

    tags: list[EntityTag] = []
    for record in records:
        node = record.get("p")
        if node is None:
            continue

        try:
            tag = _node_to_entity_tag(self, node)
        except Exception as e:
            raise e

        if tag.prop is None:
            raise Exception("malformed entity tag")
        
        if len(names) > 0:
            n = tag.prop.name
            found = n in names
            if not found:
                continue

        if tag:
            tags.append(tag)


    if len(tags) == 0:
        raise Exception("no entity tag found")

    return tags

def delete_entity_tag(self, id: str) -> EntityTag:
    tag = self.find_entity_tag_by_id(id)

    try:
        self.db.execute_query(
            "MATCH (n:EntityTag {tag_id: $id}) DETACH DELETE n",
            {"id": id})
    except Exception as e:
        raise e

    self._emit(events.EntityTagDeleted(old_tag=tag))
    return tag
