# Phase 2B: State Management Pipeline Selection Fix

**Status**: 🔄 IN PROGRESS

## Problem Statement

The current `StateManager.get_manifests_to_process()` method returns ALL manifests in "discovered" state, but the pipeline workflow requires selecting only the LATEST manifest per billing month when multiple versions exist.

**Current Behavior**:
```python
def get_manifests_to_process(self, vendor: str) -> List[StateRecord]:
    # Returns ALL discovered manifests - problematic for pipeline
```

**Required Behavior**:
- Return only 1 manifest per billing month
- Select the one with the most recent `created_at` timestamp
- Handle edge cases (ties, missing data)

## Implementation Strategy

### 1. TDD Test Cases (Write First)
```python
def test_get_pipeline_ready_manifests_latest_per_month():
    # Multiple manifests for same month -> should return latest only

def test_get_pipeline_ready_manifests_different_months():
    # Different months -> should return all

def test_get_pipeline_ready_manifests_tie_timestamps():
    # Same created_at -> deterministic selection
```

### 2. Implementation Options

**Option A**: Modify existing method (breaking change)
- Pros: Single source of truth
- Cons: May break existing usage

**Option B**: Add new method `get_pipeline_ready_manifests()` (recommended)
- Pros: Backward compatible, clear intent
- Cons: Multiple similar methods

### 3. SQL Query Design
```sql
WITH ranked_manifests AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY billing_month
               ORDER BY created_at DESC, billing_version_id DESC
           ) as rn
    FROM billing_state
    WHERE vendor = ? AND state = 'discovered'
)
SELECT * FROM ranked_manifests WHERE rn = 1
ORDER BY billing_month;
```

### 4. Error Handling
- Empty result set (no discovered manifests)
- Invalid billing_month formats
- Database connection failures

## File Changes Required
- `src/finops/state/manager.py`: Add new method
- `tests/test_state_manager.py`: Add comprehensive test cases
- Integration with pipeline orchestration (future phases)

## Success Criteria
- ✅ Only latest manifest per billing month returned
- ✅ Deterministic selection for edge cases
- ✅ Backward compatibility maintained
- ✅ Comprehensive test coverage
- ✅ Clear documentation and method naming

## Dependencies
- No additional dependencies required
- Uses existing SQLite database structure
- Compatible with existing state management patterns

## Testing Strategy
1. Unit tests for SQL query logic
2. Integration tests with multiple manifest scenarios
3. Edge case testing (empty results, ties)
4. Performance testing with large datasets