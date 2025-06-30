"""
Unit tests for the SPORC constants module.
"""

import pytest
from sporc.constants import (
    get_main_category,
    get_subcategories,
    is_main_category,
    is_subcategory,
    is_valid_category,
    get_all_categories,
    get_main_categories,
    get_subcategories_list,
    get_subcategories_by_main_category,
    get_subcategories_with_episodes,
    get_subcategory_statistics,
    search_subcategories,
    get_popular_subcategories,
    SUPPORTED_LANGUAGES,
    LANGUAGE_CODES,
    LANGUAGE_NAMES,
    APPLE_PODCAST_CATEGORIES,
    SUBCATEGORY_TO_MAIN,
    MAIN_CATEGORIES,
    SUBCATEGORIES,
    ALL_CATEGORIES
)


class TestCategoryFunctions:
    """Test category-related functions."""

    def test_get_main_category_with_subcategory(self):
        """Test get_main_category with a valid subcategory."""
        # Test with a known subcategory
        result = get_main_category("Language Learning")
        assert result == "Education"

    def test_get_main_category_with_main_category(self):
        """Test get_main_category with a main category."""
        # Test with a main category (should return itself)
        result = get_main_category("Education")
        assert result == "Education"

    def test_get_main_category_with_unknown_category(self):
        """Test get_main_category with an unknown category."""
        # Test with an unknown category (should return the category itself)
        result = get_main_category("Unknown Category")
        assert result == "Unknown Category"

    def test_get_subcategories_with_valid_main_category(self):
        """Test get_subcategories with a valid main category."""
        result = get_subcategories("Education")
        assert isinstance(result, list)
        assert "Language Learning" in result
        assert "Self-Improvement" in result

    def test_get_subcategories_with_invalid_main_category(self):
        """Test get_subcategories with an invalid main category."""
        result = get_subcategories("Invalid Category")
        assert result == []

    def test_is_main_category_with_valid_main_category(self):
        """Test is_main_category with a valid main category."""
        assert is_main_category("Education") is True
        assert is_main_category("Technology") is True

    def test_is_main_category_with_subcategory(self):
        """Test is_main_category with a subcategory."""
        assert is_main_category("Language Learning") is False

    def test_is_main_category_with_invalid_category(self):
        """Test is_main_category with an invalid category."""
        assert is_main_category("Invalid Category") is False

    def test_is_subcategory_with_valid_subcategory(self):
        """Test is_subcategory with a valid subcategory."""
        assert is_subcategory("Language Learning") is True
        assert is_subcategory("Self-Improvement") is True

    def test_is_subcategory_with_main_category(self):
        """Test is_subcategory with a main category."""
        assert is_subcategory("Education") is False

    def test_is_subcategory_with_invalid_category(self):
        """Test is_subcategory with an invalid category."""
        assert is_subcategory("Invalid Category") is False

    def test_is_valid_category_with_main_category(self):
        """Test is_valid_category with a main category."""
        assert is_valid_category("Education") is True
        assert is_valid_category("Technology") is True

    def test_is_valid_category_with_subcategory(self):
        """Test is_valid_category with a subcategory."""
        assert is_valid_category("Language Learning") is True
        assert is_valid_category("Self-Improvement") is True

    def test_is_valid_category_with_invalid_category(self):
        """Test is_valid_category with an invalid category."""
        assert is_valid_category("Invalid Category") is False

    def test_get_all_categories(self):
        """Test get_all_categories returns all categories."""
        result = get_all_categories()
        assert isinstance(result, list)
        assert len(result) > 0
        assert "Education" in result
        assert "Language Learning" in result
        # Should be sorted
        assert result == sorted(result)

    def test_get_main_categories(self):
        """Test get_main_categories returns only main categories."""
        result = get_main_categories()
        assert isinstance(result, list)
        assert len(result) > 0
        assert "Education" in result
        assert "Technology" in result
        assert "Language Learning" not in result  # Should not include subcategories
        # Should be sorted
        assert result == sorted(result)

    def test_get_subcategories_list(self):
        """Test get_subcategories_list returns only subcategories."""
        result = get_subcategories_list()
        assert isinstance(result, list)
        assert len(result) > 0
        assert "Language Learning" in result
        assert "Self-Improvement" in result
        assert "Education" not in result  # Should not include main categories
        # Should be sorted
        assert result == sorted(result)

    def test_get_subcategories_by_main_category_with_valid_category(self):
        """Test get_subcategories_by_main_category with valid main category."""
        result = get_subcategories_by_main_category("Education")
        assert isinstance(result, list)
        assert "Language Learning" in result
        assert "Self-Improvement" in result

    def test_get_subcategories_by_main_category_with_invalid_category(self):
        """Test get_subcategories_by_main_category with invalid main category."""
        result = get_subcategories_by_main_category("Invalid Category")
        assert result == []

    def test_get_subcategories_with_episodes(self):
        """Test get_subcategories_with_episodes returns related subcategories."""
        result = get_subcategories_with_episodes("Language Learning")
        assert isinstance(result, list)
        # Should return all subcategories in the same main category
        assert "Self-Improvement" in result
        assert "Language Learning" in result

    def test_get_subcategory_statistics(self):
        """Test get_subcategory_statistics returns proper statistics."""
        result = get_subcategory_statistics()
        assert isinstance(result, dict)
        assert 'total_subcategories' in result
        assert 'subcategories_by_main_category' in result
        assert 'subcategory_details' in result

        assert isinstance(result['total_subcategories'], int)
        assert result['total_subcategories'] > 0

        assert isinstance(result['subcategories_by_main_category'], dict)
        assert 'Education' in result['subcategories_by_main_category']

        assert isinstance(result['subcategory_details'], dict)
        assert 'Language Learning' in result['subcategory_details']

    def test_search_subcategories_exact_match(self):
        """Test search_subcategories with exact match."""
        result = search_subcategories("Language Learning")
        assert "Language Learning" in result

    def test_search_subcategories_partial_match(self):
        """Test search_subcategories with partial match."""
        result = search_subcategories("Language")
        assert "Language Learning" in result
        assert len(result) > 0

    def test_search_subcategories_case_insensitive(self):
        """Test search_subcategories is case insensitive."""
        result_lower = search_subcategories("language")
        result_upper = search_subcategories("LANGUAGE")
        assert result_lower == result_upper

    def test_search_subcategories_no_match(self):
        """Test search_subcategories with no match."""
        result = search_subcategories("NonexistentCategory")
        assert result == []

    def test_get_popular_subcategories(self):
        """Test get_popular_subcategories returns expected list."""
        result = get_popular_subcategories()
        assert isinstance(result, list)
        assert len(result) > 0
        assert "Language Learning" in result
        assert "Self-Improvement" in result
        assert "Entrepreneurship" in result


class TestLanguageConstants:
    """Test language-related constants."""

    def test_supported_languages_structure(self):
        """Test SUPPORTED_LANGUAGES has expected structure."""
        assert isinstance(SUPPORTED_LANGUAGES, dict)
        assert len(SUPPORTED_LANGUAGES) > 0
        assert "en" in SUPPORTED_LANGUAGES
        assert SUPPORTED_LANGUAGES["en"] == "English"

    def test_language_codes_mapping(self):
        """Test LANGUAGE_CODES mapping."""
        assert isinstance(LANGUAGE_CODES, dict)
        assert LANGUAGE_CODES["en"] == "English"
        assert LANGUAGE_CODES["es"] == "Spanish"

    def test_language_names_mapping(self):
        """Test LANGUAGE_NAMES mapping."""
        assert isinstance(LANGUAGE_NAMES, dict)
        assert LANGUAGE_NAMES["English"] == "en"
        assert LANGUAGE_NAMES["Spanish"] == "es"

    def test_language_mappings_consistency(self):
        """Test that language code-to-name mappings are consistent."""
        for code, name in SUPPORTED_LANGUAGES.items():
            assert LANGUAGE_CODES[code] == name
        # Optionally, check that all codes in LANGUAGE_NAMES values exist in SUPPORTED_LANGUAGES
        for name, code in LANGUAGE_NAMES.items():
            assert code in SUPPORTED_LANGUAGES


class TestCategoryConstants:
    """Test category-related constants."""

    def test_apple_podcast_categories_structure(self):
        """Test APPLE_PODCAST_CATEGORIES has expected structure."""
        assert isinstance(APPLE_PODCAST_CATEGORIES, dict)
        assert len(APPLE_PODCAST_CATEGORIES) > 0
        assert "Education" in APPLE_PODCAST_CATEGORIES
        assert isinstance(APPLE_PODCAST_CATEGORIES["Education"], list)

    def test_subcategory_to_main_mapping(self):
        """Test SUBCATEGORY_TO_MAIN mapping."""
        assert isinstance(SUBCATEGORY_TO_MAIN, dict)
        assert SUBCATEGORY_TO_MAIN["Language Learning"] == "Education"
        assert SUBCATEGORY_TO_MAIN["Self-Improvement"] == "Education"

    def test_main_categories_set(self):
        """Test MAIN_CATEGORIES set."""
        assert isinstance(MAIN_CATEGORIES, set)
        assert "Education" in MAIN_CATEGORIES
        assert "Technology" in MAIN_CATEGORIES

    def test_subcategories_set(self):
        """Test SUBCATEGORIES set."""
        assert isinstance(SUBCATEGORIES, set)
        assert "Language Learning" in SUBCATEGORIES
        assert "Self-Improvement" in SUBCATEGORIES

    def test_all_categories_set(self):
        """Test ALL_CATEGORIES set."""
        assert isinstance(ALL_CATEGORIES, set)
        assert "Education" in ALL_CATEGORIES
        assert "Language Learning" in ALL_CATEGORIES
        assert len(ALL_CATEGORIES) == len(MAIN_CATEGORIES) + len(SUBCATEGORIES)

    def test_category_constants_consistency(self):
        """Test that category constants are consistent."""
        # All main categories should be in APPLE_PODCAST_CATEGORIES
        for category in MAIN_CATEGORIES:
            assert category in APPLE_PODCAST_CATEGORIES

        # All subcategories should have a main category mapping
        for subcategory in SUBCATEGORIES:
            assert subcategory in SUBCATEGORY_TO_MAIN

        # All categories in APPLE_PODCAST_CATEGORIES should be main categories
        for category in APPLE_PODCAST_CATEGORIES:
            assert category in MAIN_CATEGORIES


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_get_main_category_empty_string(self):
        """Test get_main_category with empty string."""
        result = get_main_category("")
        assert result == ""

    def test_get_main_category_none(self):
        """Test get_main_category with None."""
        result = get_main_category(None)
        assert result is None

    def test_get_subcategories_empty_string(self):
        """Test get_subcategories with empty string."""
        result = get_subcategories("")
        assert result == []

    def test_get_subcategories_none(self):
        """Test get_subcategories with None."""
        result = get_subcategories(None)
        assert result == []

    def test_is_main_category_empty_string(self):
        """Test is_main_category with empty string."""
        assert is_main_category("") is False

    def test_is_main_category_none(self):
        """Test is_main_category with None."""
        assert is_main_category(None) is False

    def test_is_subcategory_empty_string(self):
        """Test is_subcategory with empty string."""
        assert is_subcategory("") is False

    def test_is_subcategory_none(self):
        """Test is_subcategory with None."""
        assert is_subcategory(None) is False

    def test_is_valid_category_empty_string(self):
        """Test is_valid_category with empty string."""
        assert is_valid_category("") is False

    def test_is_valid_category_none(self):
        """Test is_valid_category with None."""
        assert is_valid_category(None) is False

    def test_search_subcategories_empty_string(self):
        """Test search_subcategories with empty string."""
        result = search_subcategories("")
        # Should return all subcategories when searching for empty string
        assert len(result) == len(SUBCATEGORIES)

    def test_search_subcategories_none(self):
        """Test search_subcategories with None."""
        with pytest.raises(AttributeError, match="'NoneType' object has no attribute 'lower'"):
            search_subcategories(None)

    def test_get_subcategories_with_episodes_empty_string(self):
        """Test get_subcategories_with_episodes with empty string."""
        result = get_subcategories_with_episodes("")
        assert result == []

    def test_get_subcategories_with_episodes_none(self):
        """Test get_subcategories_with_episodes with None."""
        result = get_subcategories_with_episodes(None)
        assert result == []


class TestIntegration:
    """Test integration between different functions."""

    def test_category_hierarchy_consistency(self):
        """Test that category hierarchy is consistent across functions."""
        # Get all main categories
        main_cats = get_main_categories()

        for main_cat in main_cats:
            # Get subcategories for this main category
            subcats = get_subcategories(main_cat)

            for subcat in subcats:
                # Verify that get_main_category returns the correct main category
                assert get_main_category(subcat) == main_cat

                # Verify that is_subcategory returns True
                assert is_subcategory(subcat) is True

                # Verify that is_main_category returns False
                assert is_main_category(subcat) is False

                # Verify that is_valid_category returns True
                assert is_valid_category(subcat) is True

    def test_search_and_get_consistency(self):
        """Test consistency between search and get functions."""
        # Search for a subcategory
        search_results = search_subcategories("Language")

        for result in search_results:
            # Verify that we can get the main category
            main_cat = get_main_category(result)
            assert main_cat in MAIN_CATEGORIES

            # Verify that we can get subcategories for the main category
            subcats = get_subcategories(main_cat)
            assert result in subcats

    def test_statistics_consistency(self):
        """Test that statistics are consistent."""
        stats = get_subcategory_statistics()

        # Verify total count matches
        assert stats['total_subcategories'] == len(SUBCATEGORIES)

        # Verify subcategories by main category
        for main_cat, count in stats['subcategories_by_main_category'].items():
            subcats = get_subcategories(main_cat)
            assert count == len(subcats)