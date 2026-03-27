import pytest
import pandas as pd
import sys

# Local development path setup
sys.path.insert(0, '/root/glowcart')

# Constants used for cross-layer validation
VALID_EVENT_TYPES = [
    'page_view', 'add_to_cart', 'checkout', 'payment_success', 'payment_failed'
]

def make_sample_df() -> pd.DataFrame:
    """
    Utility function to generate a valid sample DataFrame for testing.
    Acts as a base fixture for data quality assertions.
    """
    return pd.DataFrame([
        {
            'event_id': 'evt-001',
            'event_type': 'payment_success',
            'timestamp': '2025-01-01 10:00:00',
            'total_amount': 150000,
            'quantity': 2,
        },
        {
            'event_id': 'evt-002',
            'event_type': 'page_view',
            'timestamp': '2025-01-01 11:00:00',
            'total_amount': 0,
            'quantity': 0,
        }
    ])

def run_validations(df: pd.DataFrame) -> list[str]:
    """
    Core validation engine. Checks for schema completeness, nulls, 
    uniqueness, and business rule constraints.
    Returns a list of error identifiers.
    """
    errors = []
    
    # Schema validation
    for col in ['event_id', 'event_type', 'timestamp', 'total_amount']:
        if col not in df.columns:
            errors.append(f"missing_column:{col}")
            
    # Data Integrity: Nullability & Uniqueness
    if df['event_id'].isnull().any():
        errors.append("null:event_id")
    if df['event_type'].isnull().any():
        errors.append("null:event_type")
    if df['event_id'].duplicated().any():
        errors.append("duplicate:event_id")
        
    # Business Logic: Value Constraints & Ranges
    if not df['event_type'].isin(VALID_EVENT_TYPES).all():
        errors.append("invalid_value:event_type")
    if not df['total_amount'].between(0, 100_000_000).all():
        errors.append("out_of_range:total_amount")
    if not df['quantity'].between(0, 1000).all():
        errors.append("out_of_range:quantity")
        
    return errors

def test_clean_data_passes_all_checks():
    """
    Validates that compliant data correctly yields an empty error list.
    """
    df = make_sample_df()
    errors = run_validations(df)
    assert errors == [], f"Unexpected failures: {errors}"

def test_duplicate_event_id_fails():
    """
    Verifies that duplicate primary identifiers (event_id) are caught.
    """
    df = make_sample_df()
    df = pd.concat([df, df]).reset_index(drop=True)
    errors = run_validations(df)
    assert "duplicate:event_id" in errors

def test_invalid_event_type_fails():
    """
    Ensures that event types outside the predefined schema are flagged.
    """
    df = make_sample_df()
    df.loc[0, 'event_type'] = 'invalid_type'
    errors = run_validations(df)
    assert "invalid_value:event_type" in errors

def test_negative_amount_fails():
    """
    Tests the boundary constraint for financial values.
    """
    df = make_sample_df()
    df.loc[0, 'total_amount'] = -1000
    errors = run_validations(df)
    assert "out_of_range:total_amount" in errors