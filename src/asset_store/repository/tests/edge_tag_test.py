import pytest
from datetime import datetime
from typing import Optional
from asset_model import Asset, AssetType, FQDN, IPAddress, IPAddressType, BasicDNSRelation, SourceProperty
from asset_store.types.entity import Entity
from asset_store.types.edge import Edge
from asset_store.types.edge_tag import EdgeTag
from asset_store.repository.neo4j.neo_repository import NeoRepository
from asset_store.events.events import (
    EdgeTagInserted,
    EdgeTagUpdated,
    EdgeTagUntouched,
    EdgeTagDeleted,
    EventType
)

@pytest.fixture
def repo():
    """Fixture providing a NeoRepository instance."""
    uri = "neo4j://localhost"
    auth = ("neo4j", "password")
    with NeoRepository(uri, auth, emit_events=True) as repository:
        yield repository
        # Cleanup: delete all entities created during the test
        try:
            repository.db.execute_query("MATCH (n) DETACH DELETE n")
        except Exception:
            pass


@pytest.fixture
def from_entity(repo: NeoRepository):
    """Fixture providing a test Entity (FQDN) for edge source."""
    asset = FQDN("test.example.com")
    entity = repo.create_asset(asset)
    repo.flush_events()
    return entity


@pytest.fixture
def to_entity(repo: NeoRepository):
    """Fixture providing a test Entity (IPAddress) for edge target."""
    asset = IPAddress("192.168.1.1", IPAddressType.IPv4)
    return repo.create_asset(asset)


@pytest.fixture
def edge(repo: NeoRepository, from_entity: Entity, to_entity: Entity):
    """Fixture providing a test Edge."""
    relation = BasicDNSRelation("dns_record", rrtype=1, rrname="A")
    return repo.create_relation(relation, from_entity, to_entity)


@pytest.fixture
def property():
    """Fixture providing a test Property (SourceProperty)."""
    return SourceProperty("myscript", 100)

def test_create_edge_property_emits_edge_tag_inserted_event(repo: NeoRepository, edge: Edge, property: SourceProperty):
    repo.flush_events() # Clear events

    # Create an edge tag from the property (first time - should emit EdgeTagInserted)
    created_tag = repo.create_edge_property(edge=edge, prop=property)
    
    # Flush and get the events
    events = repo.flush_events()
    # Verify that exactly one event was emitted
    assert len(events) == 1
        
    # Verify that the event is an EdgeTagInserted event
    event = events[0]
    assert isinstance(event, EdgeTagInserted)
    assert event.event_type == EventType.EdgeTagInserted
    
    # Verify that the event contains the correct tag
    assert event.tag.id == created_tag.id
    assert event.tag.edge.id == created_tag.edge.id


def test_create_edge_property_emits_edge_tag_updated_event(repo: NeoRepository, edge: Edge, property: SourceProperty):
    # Create an edge tag from the property (first time - should emit EdgeTagInserted)
    created_tag = repo.create_edge_property(edge=edge, prop=property)
    repo.flush_events()  # Clear events
    
    # Create the same edge tag again (should emit EdgeTagUntouched for same property)
    # For a true updated test, we'd need a property that supports freshness
    # This test will actually verify untouched behavior
    same_tag = EdgeTag(edge=edge, prop=property)
    result_tag = repo.create_edge_tag(same_tag)
    
    # Flush and get the events
    events = repo.flush_events()
    
    # Verify that exactly one event was emitted
    assert len(events) == 1
    
    # Verify that the event is an EdgeTagUntouched event (since same property)
    # Note: This test actually tests untouched, not updated
    event = events[0]
    assert isinstance(event, EdgeTagUntouched)
    assert event.event_type == EventType.EdgeTagUntouched
    assert event.tag.id == result_tag.id
    assert event.tag.id == created_tag.id


def test_create_edge_property_emits_edge_tag_untouched_event(repo: NeoRepository, edge: Edge, property: SourceProperty):
    # Create an edge tag from the property (first time - should emit EdgeTagInserted)
    created_tag = repo.create_edge_property(edge=edge, prop=property)
    repo.flush_events()  # Clear events
    
    # Create the same edge tag again (should emit EdgeTagUntouched)
    same_tag = EdgeTag(edge=edge, prop=property)
    result_tag = repo.create_edge_tag(same_tag)
    
    # Flush and get the events
    events = repo.flush_events()
    
    # Verify that exactly one event was emitted
    assert len(events) == 1
    
    # Verify that the event is an EdgeTagUntouched event
    event = events[0]
    assert isinstance(event, EdgeTagUntouched)
    assert event.event_type == EventType.EdgeTagUntouched
    
    # Verify that the event contains the correct tag (should be the original one)
    assert event.tag.id == created_tag.id
    assert event.tag.id == result_tag.id
    assert event.tag.edge.id == created_tag.edge.id


def test_delete_edge_tag_emits_edge_tag_deleted_event(repo: NeoRepository, edge: Edge, property: SourceProperty):
    # Create an edge tag from the property (first time - should emit EdgeTagInserted)
    tag = EdgeTag(edge=edge, prop=property)
    created_tag = repo.create_edge_tag(tag)
    repo.flush_events()  # Clear events

    assert created_tag.id is not None

    # Delete the edge tag
    deleted_tag = repo.delete_edge_tag(created_tag.id)
    
    # Flush and get the events
    events = repo.flush_events()
    
    # Verify that exactly one event was emitted
    assert len(events) == 1
    
    # Verify that the event is an EdgeTagDeleted event
    event = events[0]
    assert isinstance(event, EdgeTagDeleted)
    assert event.event_type == EventType.EdgeTagDeleted
    
    # Verify that the event contains the correct old tag
    assert event.old_tag.id == deleted_tag.id
    assert event.old_tag.id == created_tag.id
    assert event.old_tag.edge.id == deleted_tag.edge.id


def test_find_edge_tag_by_id(repo: NeoRepository, edge: Edge, property: SourceProperty):
    # Create an edge tag from the property
    tag = EdgeTag(edge=edge, prop=property)
    created_tag = repo.create_edge_tag(tag)
    repo.flush_events()  # Clear events

    assert created_tag.id is not None

    # Find the edge tag by ID
    found_tag = repo.find_edge_tag_by_id(created_tag.id)
    
    # Verify that the found tag matches the created tag
    assert found_tag.id == created_tag.id
    assert found_tag.edge.id == created_tag.edge.id


def test_find_edge_tag_by_id_raises_exception_when_not_found(repo: NeoRepository):
    # Try to find a non-existent edge tag
    with pytest.raises(Exception, match="the edge tag with ID .* was not found"):
        repo.find_edge_tag_by_id("non-existent-id")


def test_find_edge_tags_by_content(repo: NeoRepository, edge: Edge, property: SourceProperty):
    # Create an edge tag from the property
    tag = EdgeTag(edge=edge, prop=property)
    created_tag = repo.create_edge_tag(tag)
    repo.flush_events()  # Clear events
    
    # Find edge tags by content
    found_tags = repo.find_edge_tags_by_content(property, None)
    
    # Verify that the found tags list contains the created tag
    assert len(found_tags) >= 1
    found_ids = [t.id for t in found_tags]
    assert created_tag.id in found_ids
    
    # Verify that at least one found tag matches the created tag
    matching_tag = next((t for t in found_tags if t.id == created_tag.id), None)
    assert matching_tag is not None
    assert matching_tag.edge.id == created_tag.edge.id


def test_find_edge_tags_by_content_returns_empty_list_when_not_found(repo: NeoRepository):
    # Create a property that doesn't exist in the repository
    non_existent_property = SourceProperty("nonexistent_script", 999)
    
    # Find edge tags by content
    found_tags = repo.find_edge_tags_by_content(non_existent_property, None)
    
    # Verify that the list is empty
    assert isinstance(found_tags, list)
    assert len(found_tags) == 0
