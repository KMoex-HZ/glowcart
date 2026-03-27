import pytest
import sys

# Local development path setup
sys.path.insert(0, '/root/glowcart')

from ingestion.kafka.consumer import validate_event

def test_valid_event():
    """
    Ensures that a fully populated and correctly structured event 
    passes validation with no errors.
    """
    event = {
        'event_type': 'payment_success',
        'user': {'name': 'Budi'},
        'product': {'name': 'Sepatu'},
        'total_amount': 150000
    }
    assert validate_event(event) == []

def test_missing_event_type():
    """
    Verifies that the validator catches missing top-level required fields.
    """
    event = {
        'user': {'name': 'Budi'},
        'product': {'name': 'Sepatu'},
        'total_amount': 150000
    }
    errors = validate_event(event)
    assert 'missing_field:event_type' in errors

def test_missing_user_name():
    """
    Checks if the validator correctly identifies missing mandatory 
    nested fields within the 'user' object.
    """
    event = {
        'event_type': 'page_view',
        'user': {},
        'product': {'name': 'Sepatu'},
        'total_amount': 0
    }
    errors = validate_event(event)
    assert 'missing_field:user.name' in errors

def test_missing_product_name():
    """
    Validates the detection of missing mandatory nested fields 
    within the 'product' object.
    """
    event = {
        'event_type': 'page_view',
        'user': {'name': 'Budi'},
        'product': {},
        'total_amount': 0
    }
    errors = validate_event(event)
    assert 'missing_field:product.name' in errors

def test_multiple_missing_fields():
    """
    Tests the system's resilience when an empty or highly 
    malformed event is processed.
    """
    event = {}
    errors = validate_event(event)
    # Ensuring at least the primary required fields are flagged
    assert len(errors) >= 3