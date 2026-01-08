from asset_db.types.entity import Entity
from oam import Asset
from oam import AssetType
from oam import get_asset_by_type
from oam import describe_oam_object
from oam import make_oam_object_from_dict
from typing import Optional
from typing import cast
from datetime import datetime
from neo4j import Result
from neo4j.time import DateTime
from neo4j.graph import Node
from uuid import uuid4
from oam import FQDN, IPAddress, IPAddressType

def node_to_entity(node: Node) -> Entity:
    id = node.get("entity_id")
    if id is None:
        raise Exception("Unable to extract 'entity_id'")

    _created_at = node.get("created_at")
    if not isinstance(_created_at, DateTime):
        raise Exception("Unable to extract 'created_at'")
    created_at = _created_at.to_native()

    _updated_at = node.get("updated_at")
    if not isinstance(_updated_at, DateTime):
        raise Exception("Unable to extract 'updated_at'")
    updated_at = _updated_at.to_native()

    _etype = node.get("etype")
    if _etype is None:
        raise Exception("Unable to extract 'etype'")

    asset_type = AssetType(_etype)

    try:
        asset_cls = get_asset_by_type(asset_type)
    except Exception as e:
        raise e

    props = describe_oam_object(asset_cls)
    d = {}
    for prop_key in props:
        prop_value = node.get(prop_key)
        if prop_value is None:
            continue
        
        d[prop_key] = prop_value

    asset = cast(Asset, make_oam_object_from_dict(asset_cls, d))

    return Entity(
        id=id,
        created_at=created_at,
        updated_at=updated_at,
        asset=asset
    )

def _create_entity(self, entity: Entity) -> Entity:
    _entity: Optional[Entity] = None

    if entity.id != None and entity.id != "":
        _entity = Entity(
            id=entity.id,
            asset=entity.asset,
            created_at=entity.created_at,
            updated_at=entity.updated_at)
    else:
        findings = self.find_entities_by_content(entity.asset)
        if len(findings) > 0:
            _entity = findings[0]

    if _entity == None:
        _entity = Entity(
            id         = str(uuid4()),
            created_at = datetime.now(),
            updated_at = datetime.now(),
            asset      = entity.asset
        )

        try:
            record = self.db.execute_query(
                f"CREATE (a:Entity:{_entity.etype} $props) RETURN a",
                {"props": _entity.to_dict()},
                result_transformer_=Result.single)
        except Exception as e:
            raise e

        if record is None:
            raise Exception("no records returned from the query")

    else:
        try:
            record = self.db.execute_query(
                f"MATCH (a:Entity:{_entity.etype} {{entity_id: $id}}) SET a=$props RETURN a",
                {"id": _entity.id, "props": _entity.to_dict()},
                result_transformer_=Result.single)
        except Exception as e:
            raise e

    return _entity

def _create_asset(self, asset: Asset) -> Entity:
    return self.create_entity(
        Entity(asset=asset))

def _find_entity_by_id(self, id: str) -> Entity:
    try:
        record = self.db.execute_query(
            f"MATCH (a:Entity {{entity_id: $id}}) RETURN a",
            {"id": id},
            result_transformer_=Result.single)
    except Exception as e:
        raise e

    if record is None:
        raise Exception("the entity with ID {id} was not found")

    node = record.get("a")
    if node is None:
        raise Exception("the record value for the node is empty")

    entity = node_to_entity(node)
    return entity

def _find_entities_by_content(self, asset: Asset, since: Optional[datetime]) -> list[Entity]:
    entities: list[Entity] = []        

    props = asset.to_dict()
    props_filters = " AND ".join([f"a.{k} = ${k}" for k in props.keys()])
    
    query = f"MATCH (a:{asset.asset_type.value}) WHERE {props_filters} RETURN a"
    if since is not None:
        query = f"MATCH (a:{asset.asset_type.value}) WHERE {props_filters} AND a.updated_at >= localDateTime('{since.isoformat()}') RETURN a"

    try:
        records, summary, keys = self.db.execute_query(query, props)
    except Exception as e:
        raise e

    if len(records) == 0:
        return entities

    for rec in records:
        node = rec.get("a")
        if node is None:
            continue

        entities.append(node_to_entity(node))

    return entities

def _find_entities_by_type(self, atype: AssetType, since: Optional[datetime]) -> list[Entity]:
    query = f"MATCH (a:{atype.value} RETURN a)"
    if since is not None:
        query = f"MATCH (a:{atype.value}) WHERE a.updated_at >= localDateTime('{since.isoformat()}') RETURN a"

    try:
        records, summary, keys = self.db.execute_query(query)
    except Exception as e:
        raise e

    if len(records) == 0:
        raise Exception("no entities of the specified type")

    results: list[Entity] = []
    for record in records:
        node = record.get("a")
        if node is None:
            raise Exception("the record value for the node is empty")

        try:
            entity = node_to_entity(node)
        except Exception as e:
            raise e

        results.append(entity)

    if len(results) == 0:
        raise Exception("no entities of the specified type")

    return results

def _delete_entity(self, id: str) -> None:
    try:
        self.db.execute_query(
            "MATCH (n:Entity {entity_id: $id}) DETACH DELETE n",
            {"id": id})
    except Exception as e:
        raise e
