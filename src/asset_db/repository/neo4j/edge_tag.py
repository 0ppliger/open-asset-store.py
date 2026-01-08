from asset_db.types.edge import Edge
from asset_db.types.edge_tag import EdgeTag
from oam import Property
from oam import PropertyType
from oam import get_property_by_type
from oam import describe_oam_object
from oam import make_oam_object_from_dict
from typing import Optional
from typing import cast
from datetime import datetime
from neo4j import Result
from neo4j.graph import Node
from neo4j.time import DateTime
from uuid import uuid4

def node_to_edge_tag(self, node: Node) -> EdgeTag:
    id = node.get("tag_id")
    if id is None:
        raise Exception("Unable to extract 'tag_id'")

    edge_id = node.get("edge_id")
    if edge_id is None:
        raise Exception("Unable to extract 'edge_id'")

    try:
        edge = self.find_edge_by_id(edge_id)
    except Exception as e:
        raise e
    
    _created_at = node.get("created_at")
    if _created_at is None:
        raise Exception("Unable to extract 'created_at'")
    created_at = _created_at.to_native()

    _updated_at = node.get("updated_at")
    if _updated_at is None:
        raise Exception("Unable to extract 'updated_at'")
    updated_at = _updated_at.to_native()

    _ttype = node.get("ttype")
    if _ttype is None:
        raise Exception("Unable to extract 'ttype'")
    property_type = PropertyType(_ttype)

    try:
        property_cls = get_property_by_type(property_type)
    except Exception as e:
        raise e

    props = describe_oam_object(property_cls)
    d = {}
    for prop_key in props:
        prop_value = node.get(prop_key)
        if prop_value is None:
            continue
        
        d[prop_key] = prop_value

    prop = cast(Property, make_oam_object_from_dict(property_cls, d))

    return EdgeTag(
        id=id,
        created_at=created_at,
        updated_at=updated_at,
        prop=prop,
        edge=edge
    )


def _create_edge_tag(self, edge: Edge, tag: EdgeTag) -> EdgeTag:

    if tag.prop is None:
        raise Exception("malformed entity tag")
    
    existing_tag = None
    if tag.id is not None and tag.id != "":
        existing_tag = EdgeTag(
            id=tag.id,
            created_at=tag.created_at,
            updated_at=datetime.now(),
            prop=tag.prop,
            edge=edge,
        )
    else:
        try:
            tags = self.find_edge_tags_by_content(tag.prop)
            for t in tags:
                if t.edge.id == edge.id:
                    existing_tag = t
                    break

            if existing_tag is not None:
                existing_tag.edge = edge
                existing_tag.prop = tag.prop
                existing_tag.updated_at = datetime.now()
        except Exception as e:
            pass

    if existing_tag is not None:

        if existing_tag.prop is None:
            raise Exception("malformed entity tag")

        if tag.prop.property_type != existing_tag.prop.property_type:
            raise Exception("the property type does not match the existing tag")

        props = existing_tag.to_dict()

        try:
            record = self.db.execute_query(
                f"MATCH (n:EdgeTag {{tag_id: $tid}}) SET n = $props RETURN n",
                {"tid": existing_tag.id, "props": props},
                result_transformer_=Result.single
            )
        except Exception as e:
            raise e

        return existing_tag

    else:
        if tag.id is None or tag.id == "":
            tag.id = str(uuid4())
        if tag.created_at is None:
            tag.created_at = datetime.now()
        if tag.updated_at is None:
            tag.updated_at = datetime.now()

        tag.edge = edge
        props = tag.to_dict()

        try:
            record = self.db.execute_query(
                f"CREATE (n:EdgeTag:{tag.prop.property_type.value} $props) RETURN n",
                {"props": props},
                result_transformer_=Result.single
            )
        except Exception as e:
            raise e
        
        return tag

def _create_edge_property(self, edge: Edge, prop: Property) -> EdgeTag:
    return self.create_edge_tag(edge, EdgeTag(edge=edge, prop=prop))

def _find_edge_tag_by_id(self, id: str) -> EdgeTag:
    try:
        result = self.db.execute_query("MATCH (p:EdgeTag {tag_id: $id}) RETURN p", {"id": id})
    except Exception as e:
        raise e

    if result is None:
        raise Exception(f"the edge tag with ID {id} was not found")

    node = result.get("p")
    if node is None:
        raise Exception("the record value for the node is empty")

    return node_to_edge_tag(self, node)

def _find_edge_tags_by_content(self, prop: Property, since: Optional[datetime] = None) -> list[EdgeTag]:
    tags: list[EdgeTag] = []

    props = prop.to_dict()
    props_filters = " AND ".join([f"p.{k} = ${k}" for k in props.keys()])

    query = f"MATCH (p:{prop.property_type.value}) WHERE {props_filters} RETURN p"
    if since is not None:
        query = f"MATCH (p:{prop.property_type.value}) WHERE {props_filters} AND p.updated_at >= localDateTime('{since.isoformat()}') RETURN p"

    try:
        records, summary, keys = self.db.execute_query(query, props)
    except Exception as e:
        raise e

    if len(records) == 0:
        raise Exception("no edge tags found")

    for record in records:
        node = record.get("p")
        if node is None:
            continue

        tag = node_to_edge_tag(self, node)
        if tag:
            tags.append(tag)

    if len(tags) == 0:
        raise Exception("no edge tag found")

    return tags

def _find_edge_tags(self, edge: Edge, since: Optional[datetime] = None, *args: str) -> list[EdgeTag]:
    names = list(args)
    query = f"MATCH (p:EdgeTag {{edge_id: '{edge.id}'}}) RETURN p"
    if since is not None:
        query = f"MATCH (p:EdgeTag {{edge_id: '{edge.id}'}}) WHERE p.updated_at >= localDateTime('{since.isoformat()}') RETURN p"

    try:
        records, summary, keys = self.db.execute_query(query)
    except Exception as e:
        raise e

    if len(records) == 0:
        raise Exception("no edge tags found")

    tags: list[EdgeTag] = []
    for record in records:
        node = record.get("p")
        if node is None:
            continue

        try:
            tag = node_to_edge_tag(self, node)
        except Exception as e:
            raise e

        if tag.prop is None:
            raise Exception("malformed edge tag")

        if len(names) > 0:
            n = tag.prop.name
            found = n in names
            if not found:
                continue

        if tag:
            tags.append(tag)


    if len(tags) == 0:
        raise Exception("no edge tag found")

    return tags

def _delete_edge_tag(self, id: str) -> None:
    try:
        self.db.execute_query(
            "MATCH (n:EdgeTag {tag_id: $id}) DETACH DELETE n",
            {"id": id})
    except Exception as e:
        raise e
