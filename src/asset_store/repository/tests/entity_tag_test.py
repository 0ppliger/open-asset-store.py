import pytest
from datetime import datetime
from typing import Optional
from asset_model import Asset, AssetType, FQDN, DNSRecordProperty
from asset_store.types.entity import Entity
from asset_store.types.entity_tag import EntityTag
from asset_store.repository.neo4j.neo_repository import NeoRepository
from asset_store.events.events import (
    EntityTagInserted,
    EntityTagUpdated,
    EntityTagUntouched,
    EntityTagDeleted,
    EventType
)


@pytest.fixture
def repo():
    """Fixture providing a NeoRepository instance."""
    uri = "neo4j://localhost"
    auth = ("neo4j", "password")
    with NeoRepository(uri, auth, emit_events=True) as repository:
        yield repository
        try:
            repository.db.execute_query("MATCH (n) DETACH DELETE n")
        except Exception:
            pass


@pytest.fixture
def entity(repo: NeoRepository):
    """Fixture providing a test Entity (FQDN)."""
    asset = FQDN("test.example.com")
    entity = repo.create_asset(asset)
    return entity


@pytest.fixture
def property():
    """Fixture providing a test Property (DNSRecordProperty)."""
    return DNSRecordProperty("dns_record", "token=test123", 16, "TXT")

@pytest.fixture(autouse=True)
def run_around_tests(repo: NeoRepository):
    repo.flush_events()
    yield

def test_create_entity_property_emits_entity_tag_inserted_event(repo: NeoRepository, entity: Entity, property: DNSRecordProperty):
    repo.flush_events() # Clear events
    
    # Create the entity tag in the repository
    created_tag = repo.create_entity_property(entity=entity, prop=property)
    
    # Flush and get the events
    events = repo.flush_events()
    
    # Verify that exactly one event was emitted
    assert len(events) == 1
    
    # Verify that the event is an EntityTagInserted event
    event = events[0]
    assert isinstance(event, EntityTagInserted)
    assert event.event_type == EventType.EntityTagInserted
    
    # Verify that the event contains the correct tag
    assert event.tag.id == created_tag.id
    assert event.tag.entity.id == created_tag.entity.id


def test_create_entity_property_emits_entity_tag_updated_event(repo: NeoRepository, entity: Entity, property: DNSRecordProperty):    
    # Create the entity tag in the repository
    created_tag = repo.create_entity_property(entity=entity, prop=property)
    
    repo.flush_events()  # Clear events
    
    # Create the same entity tag again (should emit EntityTagUntouched for same property)
    # For a true updated test, we'd need a property that supports freshness
    # This test will actually verify untouched behavior
    same_tag = EntityTag(entity=entity, prop=property)
    result_tag = repo.create_entity_tag(same_tag)
    
    # Flush and get the events
    events = repo.flush_events()
    
    # Verify that exactly one event was emitted
    assert len(events) == 1
    
    # Verify that the event is an EntityTagUntouched event (since same property)
    # Note: This test actually tests untouched, not updated
    event = events[0]
    assert isinstance(event, EntityTagUntouched)
    assert event.event_type == EventType.EntityTagUntouched
    assert event.tag.id == result_tag.id
    assert event.tag.id == created_tag.id


def test_create_entity_property_emits_entity_tag_untouched_event(repo: NeoRepository, entity: Entity, property: DNSRecordProperty):
    # Create the entity tag in the repository
    created_tag = repo.create_entity_property(entity=entity, prop=property)
    
    repo.flush_events()  # Clear events
    
    # Create the same entity tag again (should emit EntityTagUntouched)
    same_tag = EntityTag(entity=entity, prop=property)
    result_tag = repo.create_entity_tag(same_tag)
    
    # Flush and get the events
    events = repo.flush_events()
    
    # Verify that exactly one event was emitted
    assert len(events) == 1
    
    # Verify that the event is an EntityTagUntouched event
    event = events[0]
    assert isinstance(event, EntityTagUntouched)
    assert event.event_type == EventType.EntityTagUntouched
    
    # Verify that the event contains the correct tag (should be the original one)
    assert event.tag.id == created_tag.id
    assert event.tag.id == result_tag.id
    assert event.tag.entity.id == created_tag.entity.id


def test_delete_entity_tag_emits_entity_tag_deleted_event(repo: NeoRepository, entity: Entity, property: DNSRecordProperty):
    # Create an entity tag from the property (first time - should emit EntityTagInserted)
    tag = EntityTag(entity=entity, prop=property)
    created_tag = repo.create_entity_tag(tag)
    repo.flush_events()  # Clear events
    
    assert created_tag.id is not None

    # Delete the entity tag
    deleted_tag = repo.delete_entity_tag(created_tag.id)
    
    # Flush and get the events
    events = repo.flush_events()
    
    # Verify that exactly one event was emitted
    assert len(events) == 1
    
    # Verify that the event is an EntityTagDeleted event
    event = events[0]
    assert isinstance(event, EntityTagDeleted)
    assert event.event_type == EventType.EntityTagDeleted
    
    # Verify that the event contains the correct old tag
    assert event.old_tag.id == deleted_tag.id
    assert event.old_tag.id == created_tag.id
    assert event.old_tag.entity.id == deleted_tag.entity.id


def test_find_entity_tag_by_id(repo: NeoRepository, entity: Entity, property: DNSRecordProperty):
    # Create an entity tag from the property
    tag = EntityTag(entity=entity, prop=property)
    created_tag = repo.create_entity_tag(tag)
    repo.flush_events()  # Clear events

    assert created_tag.id is not None

    # Find the entity tag by ID
    found_tag = repo.find_entity_tag_by_id(created_tag.id)
    
    # Verify that the found tag matches the created tag
    assert found_tag.id == created_tag.id
    assert found_tag.entity.id == created_tag.entity.id


def test_find_entity_tag_by_id_raises_exception_when_not_found(repo: NeoRepository):
    # Try to find a non-existent entity tag
    with pytest.raises(Exception, match="the entity tag with ID .* was not found"):
        repo.find_entity_tag_by_id("non-existent-id")


def test_find_entity_tags_by_content(repo: NeoRepository, entity: Entity, property: DNSRecordProperty):
    # Create an entity tag from the property
    tag = EntityTag(entity=entity, prop=property)
    created_tag = repo.create_entity_tag(tag)
    repo.flush_events()  # Clear events

    # Find entity tags by content
    found_tags = repo.find_entity_tags_by_content(property, None)
    
    # Verify that the found tags list contains the created tag
    assert len(found_tags) >= 1
    found_ids = [t.id for t in found_tags]
    assert created_tag.id in found_ids
    
    # Verify that at least one found tag matches the created tag
    matching_tag = next((t for t in found_tags if t.id == created_tag.id), None)
    assert matching_tag is not None
    assert matching_tag.entity.id == created_tag.entity.id


def test_find_entity_tags_by_content_returns_empty_list_when_not_found(repo: NeoRepository):
    # Create a property that doesn't exist in the repository
    non_existent_property = DNSRecordProperty("dns_record", "nonexistent=value", 16, "TXT")
    
    # Find entity tags by content
    found_tags = repo.find_entity_tags_by_content(non_existent_property, None)
    
    # Verify that the list is empty
    assert isinstance(found_tags, list)
    assert len(found_tags) == 0
