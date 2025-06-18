# Podcast Categories

This guide explains the podcast category system used in SPORC and how to effectively search and filter by categories.

## Overview

SPORC uses the official [Apple Podcasts categories](https://podcasters.apple.com/support/1691-apple-podcasts-categories) for organizing and searching podcast content. The category system consists of main categories and subcategories, providing a hierarchical structure for content classification.

## Category Structure

### Main Categories

The following are the main categories available in Apple Podcasts:

- **Arts** - Creative and artistic content
- **Business** - Business, finance, and professional topics
- **Comedy** - Humor and entertainment
- **Education** - Learning and educational content
- **Fiction** - Fictional storytelling
- **Government** - Government and political content
- **History** - Historical content
- **Health & Fitness** - Health, wellness, and fitness
- **Kids & Family** - Content for children and families
- **Leisure** - Hobbies, games, and recreational activities
- **Music** - Music-related content
- **News** - News and current events
- **Religion & Spirituality** - Religious and spiritual content
- **Science** - Scientific and technical content
- **Society & Culture** - Social and cultural topics
- **Sports** - Sports and athletics
- **Technology** - Technology and computing
- **True Crime** - True crime and investigative content
- **TV & Film** - Television and film-related content

### Subcategories

Each main category may have specific subcategories that provide more detailed classification:

#### Arts
- Books
- Design
- Fashion & Beauty
- Food
- Performing Arts
- Visual Arts

#### Business
- Careers
- Entrepreneurship
- Investing
- Management
- Marketing
- Non-Profit

#### Comedy
- Comedy Interviews
- Improv
- Stand-Up

#### Education
- Courses
- How To
- Language Learning
- Self-Improvement

#### Fiction
- Comedy Fiction
- Drama
- Science Fiction

#### Health & Fitness
- Alternative Health
- Fitness
- Medicine
- Mental Health
- Nutrition
- Sexuality

#### Kids & Family
- Education for Kids
- Parenting
- Pets & Animals
- Stories for Kids

#### Leisure
- Animation & Manga
- Automotive
- Aviation
- Crafts
- Games
- Hobbies
- Home & Garden
- Video Games

#### Music
- Music Commentary
- Music History
- Music Interviews

#### News
- Business News
- Daily News
- Entertainment News
- News Commentary
- Politics
- Sports News
- Tech News

#### Religion & Spirituality
- Buddhism
- Christianity
- Hinduism
- Islam
- Judaism
- Religion
- Spirituality

#### Science
- Astronomy
- Chemistry
- Earth Sciences
- Life Sciences
- Mathematics
- Natural Sciences
- Nature
- Physics
- Social Sciences

#### Society & Culture
- Documentary
- Personal Journals
- Philosophy
- Places & Travel
- Relationships

#### Sports
- Baseball
- Basketball
- Cricket
- Fantasy Sports
- Football
- Golf
- Hockey
- Rugby
- Running
- Soccer
- Swimming
- Tennis
- Volleyball
- Wilderness
- Wrestling

#### TV & Film
- After Shows
- Film History
- Film Interviews
- Film Reviews
- TV Reviews

## Using Categories in SPORC

### Basic Category Search

```python
from sporc import SPORCDataset

# Initialize dataset
sporc = SPORCDataset()

# Search by main category
education_episodes = sporc.search_episodes(category="Education")
print(f"Found {len(education_episodes)} education episodes")

# Search by subcategory
language_learning = sporc.search_episodes(category="Language Learning")
print(f"Found {len(language_learning)} language learning episodes")
```

### Multiple Category Search

```python
# Search for episodes in multiple categories
business_science = sporc.search_episodes(
    category=["Business", "Science"]
)
print(f"Found {len(business_science)} business or science episodes")

# Search for episodes in multiple subcategories
tech_content = sporc.search_episodes(
    category=["Tech News", "Technology", "Science"]
)
print(f"Found {len(tech_content)} technology-related episodes")
```

### Category with Other Criteria

```python
# Find long education episodes
long_education = sporc.search_episodes(
    category="Education",
    min_duration=1800  # 30+ minutes
)
print(f"Found {len(long_education)} long education episodes")

# Find high-quality science episodes
good_science = sporc.search_episodes(
    category="Science",
    max_overlap_prop_duration=0.1,  # Good diarization
    min_duration=1800
)
print(f"Found {len(good_science)} high-quality science episodes")
```

### Subcategory-Specific Searches

```python
# Use dedicated subcategory search methods
language_episodes = sporc.search_episodes_by_subcategory("Language Learning")
print(f"Found {len(language_episodes)} language learning episodes")

# Search for podcasts by subcategory
language_podcasts = sporc.search_podcasts_by_subcategory("Language Learning")
print(f"Found {len(language_podcasts)} podcasts with language learning content")

# Combine subcategory with other criteria
long_language_episodes = sporc.search_episodes_by_subcategory(
    "Language Learning",
    min_duration=1800,  # 30+ minutes
    min_speakers=2      # Multiple speakers
)
print(f"Found {len(long_language_episodes)} long language learning episodes with multiple speakers")

# Search for health subcategories
health_subcategories = ["Mental Health", "Nutrition", "Fitness"]
health_episodes = []
for subcategory in health_subcategories:
    episodes = sporc.search_episodes_by_subcategory(subcategory)
    health_episodes.extend(episodes)
print(f"Found {len(health_episodes)} health-related episodes")

# Search for business subcategories
business_subcategories = ["Entrepreneurship", "Investing", "Management"]
business_episodes = []
for subcategory in business_subcategories:
    episodes = sporc.search_episodes_by_subcategory(subcategory)
    business_episodes.extend(episodes)
print(f"Found {len(business_episodes)} business-related episodes")
```

## Category Utility Functions

SPORC provides several utility functions for working with categories:

### Getting Category Information

```python
from sporc import (
    get_all_categories,
    get_main_categories,
    get_subcategories_list,
    get_subcategories,
    get_main_category,
    is_main_category,
    is_subcategory,
    is_valid_category
)

# Get all available categories
all_categories = get_all_categories()
print(f"Total categories: {len(all_categories)}")

# Get main categories only
main_categories = get_main_categories()
print(f"Main categories: {len(main_categories)}")

# Get subcategories only
subcategories = get_subcategories_list()
print(f"Subcategories: {len(subcategories)}")

# Get subcategories for a specific main category
science_subcategories = get_subcategories("Science")
print(f"Science subcategories: {science_subcategories}")

# Get the main category for a subcategory
astronomy_main = get_main_category("Astronomy")
print(f"'Astronomy' belongs to: {astronomy_main}")  # Output: Science

language_main = get_main_category("Language Learning")
print(f"'Language Learning' belongs to: {language_main}")
print()

# Demonstrate subcategory utility functions
print("5. Subcategory Utility Functions:")

# Get subcategories by main category
science_subcategories = get_subcategories_by_main_category("Science")
print(f"Science subcategories: {science_subcategories}")

# Get related subcategories
related_subcategories = get_subcategories_with_episodes("Astronomy")
print(f"Subcategories related to Astronomy: {related_subcategories}")

# Search for subcategories
tech_matches = search_subcategories("tech")
print(f"Subcategories containing 'tech': {tech_matches}")

# Get popular subcategories
popular_subcategories = get_popular_subcategories()
print(f"Popular subcategories: {popular_subcategories[:5]}")

# Get subcategory statistics
subcategory_stats = get_subcategory_statistics()
print(f"Total subcategories: {subcategory_stats['total_subcategories']}")
print()
```

### Category Validation

```python
# Check if a category is valid
print(is_valid_category("Education"))      # True
print(is_valid_category("Astronomy"))      # True
print(is_valid_category("Invalid"))        # False

# Check if it's a main category
print(is_main_category("Education"))       # True
print(is_main_category("Astronomy"))       # False

# Check if it's a subcategory
print(is_subcategory("Education"))         # False
print(is_subcategory("Astronomy"))         # True
```

## Category-Based Analysis

### Category Distribution Analysis

```python
# Get dataset statistics
stats = sporc.get_dataset_statistics()

# Analyze category distribution
print("Category Distribution:")
for category, count in sorted(stats['category_distribution'].items(),
                             key=lambda x: x[1], reverse=True)[:10]:
    print(f"  {category}: {count} episodes")
```

### Cross-Category Analysis

```python
# Compare episodes across different categories
categories_to_compare = ["Education", "Science", "Business", "News"]

for category in categories_to_compare:
    episodes = sporc.search_episodes(category=category)
    if episodes:
        avg_duration = sum(ep.duration_minutes for ep in episodes) / len(episodes)
        print(f"{category}: {len(episodes)} episodes, avg {avg_duration:.1f} minutes")
```

### Subcategory Analysis

```python
# Analyze subcategories within a main category
science_episodes = sporc.search_episodes(category="Science")
science_subcategories = get_subcategories("Science")

print("Science subcategory analysis:")
for subcategory in science_subcategories:
    subcategory_episodes = sporc.search_episodes(category=subcategory)
    if subcategory_episodes:
        print(f"  {subcategory}: {len(subcategory_episodes)} episodes")
```

## Selective Loading by Category

### Load Specific Categories

```python
# Load only education and science podcasts
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(categories=['Education', 'Science'])

# Now fast searches within these categories
long_education = sporc.search_episodes(
    category="Education",
    min_duration=1800
)
print(f"Found {len(long_education)} long education episodes")
```

### Load by Subcategories

```python
# Load podcasts from specific subcategories
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(
    categories=['Language Learning', 'Self-Improvement', 'Mental Health']
)

# Search within loaded subset
language_episodes = sporc.search_episodes(category="Language Learning")
print(f"Found {len(language_episodes)} language learning episodes")
```

## Research Applications

### Content Analysis by Category

```python
def analyze_category_content(category_name):
    """Analyze content characteristics for a specific category."""
    episodes = sporc.search_episodes(category=category_name)

    if not episodes:
        return None

    # Calculate statistics
    total_episodes = len(episodes)
    total_duration = sum(ep.duration_hours for ep in episodes)
    avg_duration = total_duration / total_episodes

    # Speaker analysis
    speaker_counts = [ep.speaker_count for ep in episodes]
    avg_speakers = sum(speaker_counts) / len(speaker_counts)

    # Quality analysis
    good_quality = [ep for ep in episodes if ep.overlap_prop_duration < 0.1]
    quality_percentage = len(good_quality) / total_episodes * 100

    return {
        'category': category_name,
        'total_episodes': total_episodes,
        'total_duration_hours': total_duration,
        'avg_duration_hours': avg_duration,
        'avg_speakers': avg_speakers,
        'quality_percentage': quality_percentage
    }

# Analyze multiple categories
categories_to_analyze = ["Education", "Science", "Business", "News"]
for category in categories_to_analyze:
    analysis = analyze_category_content(category)
    if analysis:
        print(f"\n{analysis['category']}:")
        print(f"  Episodes: {analysis['total_episodes']}")
        print(f"  Total duration: {analysis['total_duration_hours']:.1f} hours")
        print(f"  Avg duration: {analysis['avg_duration_hours']:.1f} hours")
        print(f"  Avg speakers: {analysis['avg_speakers']:.1f}")
        print(f"  Good quality: {analysis['quality_percentage']:.1f}%")
```

### Category Comparison

```python
def compare_categories(category1, category2):
    """Compare two categories across multiple metrics."""
    episodes1 = sporc.search_episodes(category=category1)
    episodes2 = sporc.search_episodes(category=category2)

    def get_stats(episodes):
        if not episodes:
            return None

        durations = [ep.duration_minutes for ep in episodes]
        speakers = [ep.speaker_count for ep in episodes]
        quality = [ep.overlap_prop_duration for ep in episodes]

        return {
            'count': len(episodes),
            'avg_duration': sum(durations) / len(durations),
            'avg_speakers': sum(speakers) / len(speakers),
            'avg_quality': sum(quality) / len(quality)
        }

    stats1 = get_stats(episodes1)
    stats2 = get_stats(episodes2)

    if stats1 and stats2:
        print(f"Comparison: {category1} vs {category2}")
        print(f"  Episode count: {stats1['count']} vs {stats2['count']}")
        print(f"  Avg duration: {stats1['avg_duration']:.1f} vs {stats2['avg_duration']:.1f} minutes")
        print(f"  Avg speakers: {stats1['avg_speakers']:.1f} vs {stats2['avg_speakers']:.1f}")
        print(f"  Avg quality: {stats1['avg_quality']:.3f} vs {stats2['avg_quality']:.3f}")

# Compare some categories
compare_categories("Education", "Science")
compare_categories("Business", "News")
compare_categories("Comedy", "Fiction")
```

## Best Practices

### Category Search Tips

1. **Use Exact Names**: Category names are case-sensitive and must match exactly
2. **Check Validity**: Use `is_valid_category()` to verify category names
3. **Combine with Other Criteria**: Use categories with duration, speaker count, and quality filters
4. **Use Subcategories**: Leverage specific subcategories for more targeted searches
5. **Multiple Categories**: Use lists for searching across multiple categories

### Performance Considerations

1. **Selective Loading**: Load specific categories for focused analysis
2. **Cache Results**: Store category search results for repeated analysis
3. **Use Streaming**: For large category searches, use streaming mode
4. **Limit Results**: Use result limits for large category datasets

### Research Applications

1. **Content Analysis**: Analyze content characteristics by category
2. **Cross-Category Comparison**: Compare metrics across different categories
3. **Subcategory Analysis**: Deep dive into specific subcategories
4. **Quality Assessment**: Evaluate content quality by category
5. **Trend Analysis**: Track changes in category popularity over time

## Common Category Combinations

### Educational Content
```python
educational_categories = [
    "Education",
    "Language Learning",
    "Self-Improvement",
    "Courses",
    "How To"
]
```

### Business and Professional
```python
business_categories = [
    "Business",
    "Entrepreneurship",
    "Investing",
    "Management",
    "Marketing",
    "Careers"
]
```

### Science and Technology
```python
tech_science_categories = [
    "Science",
    "Technology",
    "Astronomy",
    "Physics",
    "Chemistry",
    "Tech News"
]
```

### Health and Wellness
```python
health_categories = [
    "Health & Fitness",
    "Mental Health",
    "Nutrition",
    "Alternative Health",
    "Medicine"
]
```

### Entertainment
```python
entertainment_categories = [
    "Comedy",
    "Music",
    "TV & Film",
    "Fiction",
    "Arts"
]
```

### News and Current Events
```python
news_categories = [
    "News",
    "Politics",
    "Business News",
    "Tech News",
    "Sports News"
]
```

This comprehensive category system allows for precise content discovery and analysis across the diverse landscape of podcast content available in the SPORC dataset.