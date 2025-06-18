#!/usr/bin/env python3
"""
Category Examples for SPORC

This script demonstrates how to use the Apple Podcasts categories
with the SPORC package, including main categories, subcategories,
and utility functions.
"""

from sporc import (
    SPORCDataset,
    get_all_categories,
    get_main_categories,
    get_subcategories_list,
    get_subcategories,
    get_main_category,
    is_main_category,
    is_subcategory,
    is_valid_category,
    get_subcategories_by_main_category,
    get_subcategories_with_episodes,
    get_subcategory_statistics,
    search_subcategories,
    get_popular_subcategories,
)


def main():
    """Main function demonstrating category usage."""

    print("=== SPORC Category Examples ===\n")

    # Initialize dataset
    print("1. Loading SPORC dataset...")
    try:
        sporc = SPORCDataset()
        print(f"   ✓ Loaded dataset with {len(sporc)} episodes\n")
    except Exception as e:
        print(f"   ✗ Error loading dataset: {e}")
        print("   Please ensure you have accepted the dataset terms on Hugging Face")
        return

    # Demonstrate category utility functions
    print("2. Category Utility Functions:")

    # Get all categories
    all_categories = get_all_categories()
    main_categories = get_main_categories()
    subcategories = get_subcategories_list()

    print(f"   Total categories: {len(all_categories)}")
    print(f"   Main categories: {len(main_categories)}")
    print(f"   Subcategories: {len(subcategories)}")

    # Show some main categories
    print(f"   Sample main categories: {main_categories[:5]}")
    print(f"   Sample subcategories: {subcategories[:5]}")
    print()

    # Demonstrate category validation
    print("3. Category Validation:")
    test_categories = ["Education", "Astronomy", "Invalid Category", "Language Learning"]

    for category in test_categories:
        is_valid = is_valid_category(category)
        is_main = is_main_category(category)
        is_sub = is_subcategory(category)

        print(f"   '{category}':")
        print(f"     Valid: {is_valid}")
        print(f"     Main category: {is_main}")
        print(f"     Subcategory: {is_sub}")

    print()

    # Demonstrate category hierarchy
    print("4. Category Hierarchy:")

    # Show subcategories for Science
    science_subcategories = get_subcategories("Science")
    print(f"   Science subcategories: {science_subcategories}")

    # Show main category for a subcategory
    astronomy_main = get_main_category("Astronomy")
    print(f"   'Astronomy' belongs to: {astronomy_main}")

    language_main = get_main_category("Language Learning")
    print(f"   'Language Learning' belongs to: {language_main}")
    print()

    # Demonstrate category searches
    print("5. Category-Based Searches:")

    # Search by main category
    try:
        education_episodes = sporc.search_episodes(category="Education")
        print(f"   Education episodes: {len(education_episodes)}")

        if education_episodes:
            avg_duration = sum(ep.duration_minutes for ep in education_episodes) / len(education_episodes)
            print(f"   Average duration: {avg_duration:.1f} minutes")
    except Exception as e:
        print(f"   Error searching education episodes: {e}")

    # Search by subcategory
    try:
        language_episodes = sporc.search_episodes(category="Language Learning")
        print(f"   Language Learning episodes: {len(language_episodes)}")
    except Exception as e:
        print(f"   Error searching language learning episodes: {e}")

    # Search by multiple categories
    try:
        business_science = sporc.search_episodes(category=["Business", "Science"])
        print(f"   Business or Science episodes: {len(business_science)}")
    except Exception as e:
        print(f"   Error searching business/science episodes: {e}")

    print()

    # Demonstrate subcategory-specific searches
    print("6. Subcategory-Specific Searches:")

    # Use dedicated subcategory search methods
    try:
        language_episodes = sporc.search_episodes_by_subcategory("Language Learning")
        print(f"   Language Learning episodes (dedicated method): {len(language_episodes)}")
    except Exception as e:
        print(f"   Error with subcategory search: {e}")

    # Search for podcasts by subcategory
    try:
        language_podcasts = sporc.search_podcasts_by_subcategory("Language Learning")
        print(f"   Podcasts with Language Learning content: {len(language_podcasts)}")
    except Exception as e:
        print(f"   Error searching podcasts by subcategory: {e}")

    # Combine subcategory with other criteria
    try:
        long_language_episodes = sporc.search_episodes_by_subcategory(
            "Language Learning",
            min_duration=1800  # 30+ minutes
        )
        print(f"   Long language learning episodes: {len(long_language_episodes)}")
    except Exception as e:
        print(f"   Error with combined subcategory search: {e}")

    print()

    # Demonstrate subcategory utility functions
    print("7. Subcategory Utility Functions:")

    # Get subcategories by main category
    science_subcategories = get_subcategories_by_main_category("Science")
    print(f"   Science subcategories: {science_subcategories}")

    # Get related subcategories
    related_subcategories = get_subcategories_with_episodes("Astronomy")
    print(f"   Subcategories related to Astronomy: {related_subcategories}")

    # Search for subcategories
    tech_matches = search_subcategories("tech")
    print(f"   Subcategories containing 'tech': {tech_matches}")

    # Get popular subcategories
    popular_subcategories = get_popular_subcategories()
    print(f"   Popular subcategories: {popular_subcategories[:5]}")

    # Get subcategory statistics
    subcategory_stats = get_subcategory_statistics()
    print(f"   Total subcategories: {subcategory_stats['total_subcategories']}")

    print()

    # Demonstrate category analysis
    print("8. Category Analysis:")

    categories_to_analyze = ["Education", "Science", "Business", "News"]

    for category in categories_to_analyze:
        try:
            episodes = sporc.search_episodes(category=category)
            if episodes:
                avg_duration = sum(ep.duration_minutes for ep in episodes) / len(episodes)
                avg_speakers = sum(ep.speaker_count for ep in episodes) / len(episodes)

                print(f"   {category}:")
                print(f"     Episodes: {len(episodes)}")
                print(f"     Avg duration: {avg_duration:.1f} minutes")
                print(f"     Avg speakers: {avg_speakers:.1f}")
            else:
                print(f"   {category}: No episodes found")
        except Exception as e:
            print(f"   Error analyzing {category}: {e}")

    print()

    # Demonstrate selective loading by category
    print("9. Selective Loading by Category:")

    try:
        # Load only education and science podcasts
        sporc_selective = SPORCDataset(streaming=True)
        sporc_selective.load_podcast_subset(categories=['Education', 'Science'])

        print(f"   Loaded {len(sporc_selective)} episodes from Education and Science")

        # Fast searches within the subset
        long_education = sporc_selective.search_episodes(
            category="Education",
            min_duration=1800  # 30+ minutes
        )
        print(f"   Long education episodes: {len(long_education)}")

    except Exception as e:
        print(f"   Error with selective loading: {e}")

    print()

    # Demonstrate category comparison
    print("10. Category Comparison:")

    def compare_categories(category1, category2):
        """Compare two categories."""
        try:
            episodes1 = sporc.search_episodes(category=category1)
            episodes2 = sporc.search_episodes(category=category2)

            print(f"   {category1} vs {category2}:")
            print(f"     Episodes: {len(episodes1)} vs {len(episodes2)}")

            if episodes1 and episodes2:
                avg_duration1 = sum(ep.duration_minutes for ep in episodes1) / len(episodes1)
                avg_duration2 = sum(ep.duration_minutes for ep in episodes2) / len(episodes2)
                print(f"     Avg duration: {avg_duration1:.1f} vs {avg_duration2:.1f} minutes")

        except Exception as e:
            print(f"     Error comparing categories: {e}")

    compare_categories("Education", "Science")
    compare_categories("Business", "News")

    print()

    # Show available categories
    print("11. Available Categories:")
    print("   Main Categories:")
    for i, category in enumerate(main_categories, 1):
        print(f"     {i:2d}. {category}")

    print("\n   Popular Subcategories:")
    popular_subcategories = [
        "Language Learning", "Self-Improvement", "Entrepreneurship",
        "Investing", "Astronomy", "Physics", "Mental Health",
        "Nutrition", "Tech News", "Politics"
    ]

    for i, subcategory in enumerate(popular_subcategories, 1):
        main_cat = get_main_category(subcategory)
        print(f"     {i:2d}. {subcategory} ({main_cat})")

    print("\n   Subcategory Examples by Main Category:")
    main_categories_examples = ["Science", "Business", "Health & Fitness", "Sports"]
    for main_cat in main_categories_examples:
        subcategories = get_subcategories_by_main_category(main_cat)
        if subcategories:
            print(f"     {main_cat}: {', '.join(subcategories[:3])}{'...' if len(subcategories) > 3 else ''}")

    print("\n=== Category Examples Complete ===")
    print("\nFor more information, see:")
    print("- Categories guide: docs/wiki/Categories.md")
    print("- Search examples: docs/wiki/Search-Examples.md")
    print("- API reference: docs/wiki/API-Reference.md")


if __name__ == "__main__":
    main()