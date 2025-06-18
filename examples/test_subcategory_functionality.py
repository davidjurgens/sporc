#!/usr/bin/env python3
"""
Test script to verify subcategory functionality in SPORC.

This script tests the new subcategory-specific methods and functions
to ensure they work correctly.
"""

from sporc import (
    get_subcategories_by_main_category,
    get_subcategories_with_episodes,
    get_subcategory_statistics,
    search_subcategories,
    get_popular_subcategories,
    is_subcategory,
    is_valid_category,
    get_main_category,
)


def test_subcategory_utility_functions():
    """Test subcategory utility functions."""
    print("Testing subcategory utility functions...")

    # Test get_subcategories_by_main_category
    science_subcategories = get_subcategories_by_main_category("Science")
    assert "Astronomy" in science_subcategories
    assert "Physics" in science_subcategories
    print("✓ get_subcategories_by_main_category works")

    # Test get_subcategories_with_episodes
    related_subcategories = get_subcategories_with_episodes("Astronomy")
    assert "Physics" in related_subcategories
    assert "Chemistry" in related_subcategories
    print("✓ get_subcategories_with_episodes works")

    # Test search_subcategories
    tech_matches = search_subcategories("tech")
    assert "Tech News" in tech_matches
    print("✓ search_subcategories works")

    # Test get_popular_subcategories
    popular = get_popular_subcategories()
    assert "Language Learning" in popular
    assert "Self-Improvement" in popular
    print("✓ get_popular_subcategories works")

    # Test get_subcategory_statistics
    stats = get_subcategory_statistics()
    assert "total_subcategories" in stats
    assert stats["total_subcategories"] > 0
    print("✓ get_subcategory_statistics works")

    # Test is_subcategory
    assert is_subcategory("Astronomy") == True
    assert is_subcategory("Science") == False
    print("✓ is_subcategory works")

    # Test is_valid_category with subcategories
    assert is_valid_category("Astronomy") == True
    assert is_valid_category("Invalid Category") == False
    print("✓ is_valid_category works with subcategories")

    # Test get_main_category
    assert get_main_category("Astronomy") == "Science"
    assert get_main_category("Language Learning") == "Education"
    print("✓ get_main_category works")

    print("All subcategory utility function tests passed!")


def test_subcategory_validation():
    """Test subcategory validation."""
    print("\nTesting subcategory validation...")

    # Test valid subcategories
    valid_subcategories = [
        "Astronomy", "Physics", "Language Learning", "Self-Improvement",
        "Entrepreneurship", "Mental Health", "Tech News", "Football"
    ]

    for subcategory in valid_subcategories:
        assert is_valid_category(subcategory), f"Subcategory {subcategory} should be valid"
        assert is_subcategory(subcategory), f"{subcategory} should be a subcategory"

    # Test invalid categories
    invalid_categories = ["Invalid Category", "Fake Subcategory", "Test"]
    for category in invalid_categories:
        assert not is_valid_category(category), f"Category {category} should be invalid"

    print("✓ Subcategory validation tests passed!")


def test_subcategory_hierarchy():
    """Test subcategory hierarchy relationships."""
    print("\nTesting subcategory hierarchy...")

    # Test main category relationships
    test_cases = [
        ("Astronomy", "Science"),
        ("Language Learning", "Education"),
        ("Entrepreneurship", "Business"),
        ("Mental Health", "Health & Fitness"),
        ("Tech News", "News"),
        ("Football", "Sports")
    ]

    for subcategory, expected_main in test_cases:
        actual_main = get_main_category(subcategory)
        assert actual_main == expected_main, f"Expected {subcategory} to belong to {expected_main}, got {actual_main}"

    print("✓ Subcategory hierarchy tests passed!")


def main():
    """Run all subcategory functionality tests."""
    print("=== SPORC Subcategory Functionality Tests ===\n")

    try:
        test_subcategory_utility_functions()
        test_subcategory_validation()
        test_subcategory_hierarchy()

        print("\n=== All Tests Passed! ===")
        print("Subcategory functionality is working correctly.")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()