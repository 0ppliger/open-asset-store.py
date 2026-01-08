from neo4j.graph import Node
from neo4j.graph import Relationship
from neo4j.time import DateTime
from oam import Property
from oam import AssetType
from oam import RelationType
from oam import FQDN
from oam import IPAddress, IPAddressType, PropertyType
from oam import BasicDNSRelation
from oam import PortRelation
from oam import PrefDNSRelation
from oam import SimpleRelation
from oam import SRVDNSRelation
from oam import RRHeader
from oam import DNSRecordProperty
from oam import SimpleProperty
from oam import SourceProperty
from oam import VulnProperty
from asset_db.types.entity import Entity
from asset_db.types.edge import Edge
from asset_db.types.entity_tag import EntityTag
from asset_db.types.edge_tag import EdgeTag

def node_to_property(ttype: PropertyType, node: Node) -> Property:
    match ttype:
        case PropertyType.DNSRecordProperty:
            return DNSRecordProperty(
                property_name = node.get("property_name"),
                header = RRHeader(
                    rr_type = node.get("header_rrtype"),
                    cls     = node.get("header_class"),
                    ttl     = node.get("header_ttl")   
                ),
                data = node.get("data")
            )
        case PropertyType.SimpleProperty:
            return SimpleProperty(
                property_name  = node.get("property_name"),
                property_value = node.get("property_value")
            )
        case PropertyType.SourceProperty:
            return SimpleProperty(
                name  = node.get("name"),
                confidence = node.get("confidence")
            )
        case PropertyType.VulnProperty:
            return VulnProperty(
                id  = node.get("id"),
                desc = node.get("desc"),
                source = node.get("source"),
                category = node.get("category"),
                enum = node.get("enum"),
                ref = node.get("ref")
            )
        case _:
            raise Exception("Unsupported property type")

# TOCHECK
def node_to_entity_tag(node: Node) -> EntityTag:
    et = EntityTag()
    
    et.id = node.get("tag_id")
    if et.id is None:
        raise Exception("Unable to extract 'tag_id'")

    eid = node.get("entity_id")
    if eid is None:
        raise Exception("Unable to extract 'entity_id'")
    et.entity = Entity(id=eid)
    
    _created_at = node.get("created_at")
    if _created_at is None:
        raise Exception("Unable to extract 'created_at'")
    et.created_at = _created_at.to_native()

    _updated_at = node.get("updated_at")
    if _updated_at is None:
        raise Exception("Unable to extract 'created_at'")
    et.updated_at = _updated_at.to_native()
    
    _ttype = node.get("ttype")
    if _ttype is None:
        raise Exception("Unable to extract 'ttype'")
    ttype = PropertyType(_ttype)
    
    et.property = node_to_property(ttype, node)
        
    return et

def node_to_edge_tag(node: Node) -> EdgeTag:
    edge_tag = EdgeTag()

    edge_tag.id = node.get("tag_id")
    if edge_tag.id is None:
        raise Exception("Unable to extract 'tag_id'")

    eid = node.get("edge_id")
    if eid is None:
        raise Exception("Unable to extract 'edge_id'")
    edge_tag.edge = Edge(id=eid)

    _created_at = node.get("created_at")
    if _created_at is None:
        raise Exception("Unable to extract 'created_at'")
    edge_tag.created_at = _created_at.to_native()

    _updated_at = node.get("updated_at")
    if _updated_at is None:
        raise Exception("Unable to extract 'updated_at'")
    edge_tag.last_seen = _updated_at.to_native()

    _ttype = node.get("ttype")
    if _ttype is None:
        raise Exception("Unable to extract 'ttype'")
    ttype = PropertyType(_ttype)

    try:
        _prop = node_to_property(ttype, node)
    except Exception as e:
        raise e
    
    edge_tag.property = _prop

    return edge_tag

# DONE!
def relationship_to_edge(rel: Relationship) -> Edge:
    edge = Edge()

    edge.id = rel.element_id

    _created_at = rel.get("created_at")
    if not isinstance(_created_at, DateTime):
        raise Exception("Unable to extract 'created_at'")
    edge.created_at = _created_at.to_native()

    _updated_at = rel.get("updated_at")
    if not isinstance(_updated_at, DateTime):
        raise Exception("Unable to extract 'updated_at'")
    edge.updated_at = _updated_at.to_native()

    _etype = rel.get("etype")
    if _etype is None:
        raise Exception("Unable to extract 'etype'")
    rtype = RelationType(_etype)

    match rtype:
        case RelationType.BasicDNSRelation:
            edge.relation = BasicDNSRelation(
                name = rel.get("label"),
                header=RRHeader(
                    rr_type = rel.get("header_rrtype"),
                    cls     = rel.get("header_class"),
                    ttl     = rel.get("header_ttl")
                )
            )
        case RelationType.PrefDNSRelation:
            edge.relation = PrefDNSRelation(
                name = rel.get("label"),
                header = RRHeader(
                    rr_type = rel.get("header_rrtype"),
                    cls     = rel.get("header_class"),
                    ttl     = rel.get("header_ttl")
                ),
                preference = rel.get("preference")
            )
        case RelationType.SRVDNSRelation:
            edge.relation = SRVDNSRelation(
                name = rel.get("label"),
                header = RRHeader(
                    rr_type = rel.get("header_rrtype"),
                    cls     = rel.get("header_class"),
                    ttl     = rel.get("header_ttl")
                ),
                priority = rel.get("priority"),
                wright   = rel.get("weight"),
                port     = rel.get("port")
            )
        case RelationType.PortRelation:
            edge.relation = PortRelation(
                name        = rel.get("label"),
                port_number = rel.get("port_number"),
                protocol    = rel.get("protocol")
            )
        case RelationType.SimpleRelation:
            edge.relation = SimpleRelation(
                name = rel.get("label")
            )
        case _:
            raise Exception("Unsupported relation type")

    return edge
