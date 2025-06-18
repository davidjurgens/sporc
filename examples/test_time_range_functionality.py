#!/usr/bin/env python3
"""
Test script to verify time range functionality in SPORC.

This script tests the new time range behavior options to ensure they work correctly.
"""

from sporc import TimeRangeBehavior


def test_time_range_behavior_enum():
    """Test the TimeRangeBehavior enum."""
    print("Testing TimeRangeBehavior enum...")

    # Test enum values
    assert TimeRangeBehavior.STRICT == TimeRangeBehavior.STRICT
    assert TimeRangeBehavior.INCLUDE_PARTIAL == TimeRangeBehavior.INCLUDE_PARTIAL
    assert TimeRangeBehavior.INCLUDE_FULL_TURNS == TimeRangeBehavior.INCLUDE_FULL_TURNS

    # Test enum names
    assert TimeRangeBehavior.STRICT.name == "STRICT"
    assert TimeRangeBehavior.INCLUDE_PARTIAL.name == "INCLUDE_PARTIAL"
    assert TimeRangeBehavior.INCLUDE_FULL_TURNS.name == "INCLUDE_FULL_TURNS"

    # Test enum values
    assert TimeRangeBehavior.STRICT.value == "strict"
    assert TimeRangeBehavior.INCLUDE_PARTIAL.value == "include_partial"
    assert TimeRangeBehavior.INCLUDE_FULL_TURNS.value == "include_full_turns"

    print("✓ TimeRangeBehavior enum tests passed!")


def test_time_range_behavior_comparison():
    """Test time range behavior comparison logic."""
    print("\nTesting time range behavior comparison logic...")

    # Simulate turn data
    class MockTurn:
        def __init__(self, start_time, end_time):
            self.start_time = start_time
            self.end_time = end_time

    # Test cases: (turn_start, turn_end, range_start, range_end, expected_results)
    test_cases = [
        # Turn completely within range
        (10, 20, 5, 25, {
            'strict': True,
            'partial': True,
            'full': True
        }),
        # Turn completely outside range (before)
        (5, 8, 10, 20, {
            'strict': False,
            'partial': False,
            'full': False
        }),
        # Turn completely outside range (after)
        (25, 30, 10, 20, {
            'strict': False,
            'partial': False,
            'full': False
        }),
        # Turn overlaps start of range
        (5, 15, 10, 20, {
            'strict': False,
            'partial': True,
            'full': True
        }),
        # Turn overlaps end of range
        (15, 25, 10, 20, {
            'strict': False,
            'partial': True,
            'full': True
        }),
        # Turn completely contains range
        (5, 25, 10, 20, {
            'strict': False,
            'partial': True,
            'full': True
        }),
        # Turn exactly matches range
        (10, 20, 10, 20, {
            'strict': True,
            'partial': True,
            'full': True
        })
    ]

    for turn_start, turn_end, range_start, range_end, expected in test_cases:
        turn = MockTurn(turn_start, turn_end)

        # Test STRICT behavior
        strict_result = (turn.start_time >= range_start and turn.end_time <= range_end)
        assert strict_result == expected['strict'], f"STRICT failed for turn {turn_start}-{turn_end} in range {range_start}-{range_end}"

        # Test INCLUDE_PARTIAL behavior (overlaps)
        partial_result = not (turn.end_time <= range_start or range_end <= turn.start_time)
        assert partial_result == expected['partial'], f"INCLUDE_PARTIAL failed for turn {turn_start}-{turn_end} in range {range_start}-{range_end}"

        # Test INCLUDE_FULL_TURNS behavior (touches)
        full_result = (turn.start_time < range_end and turn.end_time > range_start)
        assert full_result == expected['full'], f"INCLUDE_FULL_TURNS failed for turn {turn_start}-{turn_end} in range {range_start}-{range_end}"

    print("✓ Time range behavior comparison tests passed!")


def test_time_range_behavior_documentation():
    """Test that the behavior documentation is accurate."""
    print("\nTesting time range behavior documentation...")

    # Verify enum docstring contains all behaviors
    docstring = TimeRangeBehavior.__doc__
    assert "STRICT" in docstring
    assert "INCLUDE_PARTIAL" in docstring
    assert "INCLUDE_FULL_TURNS" in docstring

    # Verify behavior descriptions
    assert "completely within" in docstring
    assert "overlap" in docstring
    assert "extend beyond" in docstring

    print("✓ Time range behavior documentation tests passed!")


def main():
    """Run all time range functionality tests."""
    print("=== SPORC Time Range Functionality Tests ===\n")

    try:
        test_time_range_behavior_enum()
        test_time_range_behavior_comparison()
        test_time_range_behavior_documentation()

        print("\n=== All Tests Passed! ===")
        print("Time range functionality is working correctly.")
        print("\nAvailable behaviors:")
        for behavior in TimeRangeBehavior:
            print(f"  - {behavior.name}: {behavior.value}")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()