# Search Examples

This guide provides comprehensive examples of how to search for podcasts and episodes in the SPORC dataset using various criteria and combinations.

## Basic Search Operations

### Search by Podcast Name

```python
from sporc import SPORCDataset

# Initialize dataset
sporc = SPORCDataset()

# Search for a specific podcast by exact name
podcast = sporc.search_podcast("SingOut SpeakOut")
if podcast:
    print(f"Found: {podcast.title}")
    print(f"Episodes: {len(podcast.episodes)}")
    print(f"Category: {podcast.category}")
else:
    print("Podcast not found")

# Search for podcasts with partial name matching
all_podcasts = sporc.get_all_podcasts()
matching_podcasts = [p for p in all_podcasts if "education" in p.title.lower()]
print(f"Found {len(matching_podcasts)} podcasts with 'education' in the title")
```

### Search Episodes by Duration

```python
# Find episodes longer than 30 minutes
long_episodes = sporc.search_episodes(min_duration=1800)
print(f"Found {len(long_episodes)} episodes longer than 30 minutes")

# Find episodes between 10-30 minutes
medium_episodes = sporc.search_episodes(min_duration=600, max_duration=1800)
print(f"Found {len(medium_episodes)} episodes between 10-30 minutes")

# Find short episodes (less than 10 minutes)
short_episodes = sporc.search_episodes(max_duration=600)
print(f"Found {len(short_episodes)} episodes shorter than 10 minutes")

# Find very long episodes (more than 2 hours)
very_long_episodes = sporc.search_episodes(min_duration=7200)
print(f"Found {len(very_long_episodes)} episodes longer than 2 hours")
```

### Search Episodes by Speaker Count

```python
# Find episodes with exactly 2 speakers
two_speaker_episodes = sporc.search_episodes(min_speakers=2, max_speakers=2)
print(f"Found {len(two_speaker_episodes)} episodes with exactly 2 speakers")

# Find episodes with 3 or more speakers
multi_speaker_episodes = sporc.search_episodes(min_speakers=3)
print(f"Found {len(multi_speaker_episodes)} episodes with 3+ speakers")

# Find solo episodes (1 speaker)
solo_episodes = sporc.search_episodes(min_speakers=1, max_speakers=1)
print(f"Found {len(solo_episodes)} solo episodes")

# Find episodes with 4-6 speakers
group_episodes = sporc.search_episodes(min_speakers=4, max_speakers=6)
print(f"Found {len(group_episodes)} episodes with 4-6 speakers")
```

### Search Episodes by Host

```python
# Find episodes hosted by a specific person
simon_episodes = sporc.search_episodes(host_name="Simon Shapiro")
print(f"Found {len(simon_episodes)} episodes hosted by Simon Shapiro")

# Find episodes with multiple hosts
multi_host_episodes = sporc.search_episodes(host_name=["Simon Shapiro", "John Doe"])
print(f"Found {len(multi_host_episodes)} episodes with specified hosts")

# Search for episodes by guest
guest_episodes = sporc.search_episodes(guest_name="Jane Smith")
print(f"Found {len(guest_episodes)} episodes featuring Jane Smith")
```

### Search Episodes by Category

```python
# Find education podcasts
education_episodes = sporc.search_episodes(category="Education")
print(f"Found {len(education_episodes)} education episodes")

# Find specific education subcategories
language_learning_episodes = sporc.search_episodes(category="Language Learning")
print(f"Found {len(language_learning_episodes)} language learning episodes")

self_improvement_episodes = sporc.search_episodes(category="Self-Improvement")
print(f"Found {len(self_improvement_episodes)} self-improvement episodes")

# Find business podcasts
business_episodes = sporc.search_episodes(category="Business")
print(f"Found {len(business_episodes)} business episodes")

# Find specific business subcategories
entrepreneurship_episodes = sporc.search_episodes(category="Entrepreneurship")
print(f"Found {len(entrepreneurship_episodes)} entrepreneurship episodes")

investing_episodes = sporc.search_episodes(category="Investing")
print(f"Found {len(investing_episodes)} investing episodes")

# Find science podcasts
science_episodes = sporc.search_episodes(category="Science")
print(f"Found {len(science_episodes)} science episodes")

# Find specific science subcategories
astronomy_episodes = sporc.search_episodes(category="Astronomy")
print(f"Found {len(astronomy_episodes)} astronomy episodes")

physics_episodes = sporc.search_episodes(category="Physics")
print(f"Found {len(physics_episodes)} physics episodes")

# Find news podcasts
news_episodes = sporc.search_episodes(category="News")
print(f"Found {len(news_episodes)} news episodes")

# Find specific news subcategories
tech_news_episodes = sporc.search_episodes(category="Tech News")
print(f"Found {len(tech_news_episodes)} tech news episodes")

politics_episodes = sporc.search_episodes(category="Politics")
print(f"Found {len(politics_episodes)} politics episodes")

# Find health and fitness podcasts
health_episodes = sporc.search_episodes(category="Health & Fitness")
print(f"Found {len(health_episodes)} health and fitness episodes")

# Find specific health subcategories
mental_health_episodes = sporc.search_episodes(category="Mental Health")
print(f"Found {len(mental_health_episodes)} mental health episodes")

nutrition_episodes = sporc.search_episodes(category="Nutrition")
print(f"Found {len(nutrition_episodes)} nutrition episodes")

# Find sports podcasts
sports_episodes = sporc.search_episodes(category="Sports")
print(f"Found {len(sports_episodes)} sports episodes")

# Find specific sports subcategories
football_episodes = sporc.search_episodes(category="Football")
print(f"Found {len(football_episodes)} football episodes")

basketball_episodes = sporc.search_episodes(category="Basketball")
print(f"Found {len(basketball_episodes)} basketball episodes")

# Find leisure podcasts
leisure_episodes = sporc.search_episodes(category="Leisure")
print(f"Found {len(leisure_episodes)} leisure episodes")

# Find specific leisure subcategories
video_games_episodes = sporc.search_episodes(category="Video Games")
print(f"Found {len(video_games_episodes)} video games episodes")

automotive_episodes = sporc.search_episodes(category="Automotive")
print(f"Found {len(automotive_episodes)} automotive episodes")

# Find society and culture podcasts
society_episodes = sporc.search_episodes(category="Society & Culture")
print(f"Found {len(society_episodes)} society and culture episodes")

# Find specific society subcategories
documentary_episodes = sporc.search_episodes(category="Documentary")
print(f"Found {len(documentary_episodes)} documentary episodes")

philosophy_episodes = sporc.search_episodes(category="Philosophy")
print(f"Found {len(philosophy_episodes)} philosophy episodes")
```

### Search Episodes by Subcategory

```python
# Use the dedicated subcategory search method
language_learning_episodes = sporc.search_episodes_by_subcategory("Language Learning")
print(f"Found {len(language_learning_episodes)} language learning episodes")

# Use the subcategory parameter in general search
self_improvement_episodes = sporc.search_episodes(subcategory="Self-Improvement")
print(f"Found {len(self_improvement_episodes)} self-improvement episodes")

# Search for podcasts by subcategory
language_podcasts = sporc.search_podcasts_by_subcategory("Language Learning")
print(f"Found {len(language_podcasts)} podcasts with language learning episodes")

# Combine subcategory with other criteria
long_language_episodes = sporc.search_episodes_by_subcategory(
    "Language Learning",
    min_duration=1800  # 30+ minutes
)
print(f"Found {len(long_language_episodes)} long language learning episodes")

# Search for multiple subcategories
tech_subcategories = ["Tech News", "Technology"]
tech_episodes = []
for subcategory in tech_subcategories:
    episodes = sporc.search_episodes_by_subcategory(subcategory)
    tech_episodes.extend(episodes)
print(f"Found {len(tech_episodes)} tech-related episodes")

# Search for health subcategories
health_subcategories = ["Mental Health", "Nutrition", "Fitness"]
health_episodes = []
for subcategory in health_subcategories:
    episodes = sporc.search_episodes_by_subcategory(subcategory)
    health_episodes.extend(episodes)
print(f"Found {len(health_episodes)} health-related episodes")

# Search for sports subcategories
sports_subcategories = ["Football", "Basketball", "Baseball", "Soccer"]
sports_episodes = []
for subcategory in sports_subcategories:
    episodes = sporc.search_episodes_by_subcategory(subcategory)
    sports_episodes.extend(episodes)
print(f"Found {len(sports_episodes)} sports-related episodes")
```

### Search Episodes by Language

```python
# Find English episodes
english_episodes = sporc.search_episodes(language="en")
print(f"Found {len(english_episodes)} English episodes")

# Find Spanish episodes
spanish_episodes = sporc.search_episodes(language="es")
print(f"Found {len(spanish_episodes)} Spanish episodes")

# Find episodes in other languages
other_language_episodes = sporc.search_episodes(language=["fr", "de", "it"])
print(f"Found {len(other_language_episodes)} episodes in other languages")
```

## Advanced Search Combinations

### Complex Duration and Speaker Searches

```python
# Find long episodes with multiple speakers
long_multi_speaker = sporc.search_episodes(
    min_duration=3600,    # 1+ hour
    min_speakers=3,       # 3+ speakers
    max_speakers=6        # 6 or fewer speakers
)
print(f"Found {len(long_multi_speaker)} long episodes with 3-6 speakers")

# Find short solo episodes
short_solo = sporc.search_episodes(
    max_duration=900,     # 15 minutes or less
    min_speakers=1,       # At least 1 speaker
    max_speakers=1        # At most 1 speaker
)
print(f"Found {len(short_solo)} short solo episodes")
```

### Category and Duration Combinations

```python
# Find long education episodes
long_education = sporc.search_episodes(
    category="Education",
    min_duration=1800     # 30+ minutes
)
print(f"Found {len(long_education)} long education episodes")

# Find short music episodes
short_music = sporc.search_episodes(
    category="Music",
    max_duration=600      # 10 minutes or less
)
print(f"Found {len(short_music)} short music episodes")

# Find medium-length science episodes
medium_science = sporc.search_episodes(
    category="Science",
    min_duration=900,     # 15+ minutes
    max_duration=2700     # 45 minutes or less
)
print(f"Found {len(medium_science)} medium-length science episodes")

# Find long business episodes
long_business = sporc.search_episodes(
    category="Business",
    min_duration=3600     # 1+ hour
)
print(f"Found {len(long_business)} long business episodes")

# Find short comedy episodes
short_comedy = sporc.search_episodes(
    category="Comedy",
    max_duration=900      # 15 minutes or less
)
print(f"Found {len(short_comedy)} short comedy episodes")
```

### Host and Category Combinations

```python
# Find education episodes by specific host
simon_education = sporc.search_episodes(
    host_name="Simon Shapiro",
    category="Education"
)
print(f"Found {len(simon_education)} education episodes by Simon Shapiro")

# Find science episodes with multiple hosts
multi_host_science = sporc.search_episodes(
    host_name=["Simon Shapiro", "John Doe", "Jane Smith"],
    category="Science"
)
print(f"Found {len(multi_host_science)} science episodes by specified hosts")

# Find business episodes by specific hosts
business_hosts = sporc.search_episodes(
    host_name=["Business Host", "Finance Expert"],
    category="Business"
)
print(f"Found {len(business_hosts)} business episodes by business hosts")
```

### Language and Quality Combinations

```python
# Find high-quality English episodes
good_english = sporc.search_episodes(
    language="en",
    max_overlap_prop_duration=0.1,    # Less than 10% overlap
    max_overlap_prop_turn_count=0.2   # Less than 20% overlapping turns
)
print(f"Found {len(good_english)} high-quality English episodes")

# Find substantial Spanish content
substantial_spanish = sporc.search_episodes(
    language="es",
    min_duration=1800,                # 30+ minutes
    min_total_duration=2.0            # 2+ hours total
)
print(f"Found {len(substantial_spanish)} substantial Spanish episodes")
```

## Quality-Based Searches

### Search by Diarization Quality

```python
# Find episodes with excellent diarization
excellent_quality = sporc.search_episodes(
    max_overlap_prop_duration=0.05,   # Less than 5% overlap
    max_overlap_prop_turn_count=0.1   # Less than 10% overlapping turns
)
print(f"Found {len(excellent_quality)} episodes with excellent diarization")

# Find episodes with good diarization
good_quality = sporc.search_episodes(
    max_overlap_prop_duration=0.1,    # Less than 10% overlap
    max_overlap_prop_turn_count=0.2   # Less than 20% overlapping turns
)
print(f"Found {len(good_quality)} episodes with good diarization")

# Find episodes with moderate diarization
moderate_quality = sporc.search_episodes(
    max_overlap_prop_duration=0.2,    # Less than 20% overlap
    max_overlap_prop_turn_count=0.3   # Less than 30% overlapping turns
)
print(f"Found {len(moderate_quality)} episodes with moderate diarization")
```

### Search by Content Quality

```python
# Find episodes with substantial content
substantial_content = sporc.search_episodes(
    min_duration=1800,                # 30+ minutes
    min_total_duration=5.0            # 5+ hours total podcast content
)
print(f"Found {len(substantial_content)} episodes with substantial content")

# Find episodes from established podcasts
established_podcasts = sporc.search_episodes(
    min_episodes=10                   # Podcasts with 10+ episodes
)
print(f"Found {len(established_podcasts)} episodes from established podcasts")
```

## Research-Specific Searches

### Conversation Analysis Searches

```python
# Find episodes suitable for conversation analysis
conversation_analysis = sporc.search_episodes(
    min_speakers=2,                   # At least 2 speakers
    max_speakers=4,                   # At most 4 speakers
    min_duration=1800,                # 30+ minutes
    max_overlap_prop_duration=0.1,    # Good diarization
    category="Education"              # Education content
)
print(f"Found {len(conversation_analysis)} episodes suitable for conversation analysis")

# Find episodes for speaker role analysis
speaker_role_analysis = sporc.search_episodes(
    min_speakers=3,                   # Multiple speakers
    max_speakers=6,                   # Not too many speakers
    min_duration=2700,                # 45+ minutes
    category=["Education", "Science"] # Educational content
)
print(f"Found {len(speaker_role_analysis)} episodes for speaker role analysis")
```

### Content Analysis Searches

```python
# Find episodes for content analysis
content_analysis = sporc.search_episodes(
    min_duration=3600,                # 1+ hour episodes
    min_total_duration=10.0,          # 10+ hours total content
    language="en",                    # English content
    max_overlap_prop_duration=0.1     # Good quality
)
print(f"Found {len(content_analysis)} episodes for content analysis")

# Find episodes for topic modeling
topic_modeling = sporc.search_episodes(
    min_duration=1800,                # 30+ minutes
    min_speakers=2,                   # Multiple speakers
    category=["Education", "Science", "News"]  # Various topics
)
print(f"Found {len(topic_modeling)} episodes for topic modeling")
```

### Specialized Category Searches

```python
# Find technology-focused episodes
tech_episodes = sporc.search_episodes(
    category="Technology",
    min_duration=1800                 # 30+ minutes
)
print(f"Found {len(tech_episodes)} technology episodes")

# Find true crime episodes
true_crime_episodes = sporc.search_episodes(
    category="True Crime",
    min_duration=2700                 # 45+ minutes
)
print(f"Found {len(true_crime_episodes)} true crime episodes")

# Find fiction episodes
fiction_episodes = sporc.search_episodes(
    category="Fiction",
    min_speakers=2                    # Multiple speakers for dialogue
)
print(f"Found {len(fiction_episodes)} fiction episodes")

# Find specific fiction subcategories
drama_episodes = sporc.search_episodes(
    category="Drama",
    min_duration=1800
)
print(f"Found {len(drama_episodes)} drama episodes")

science_fiction_episodes = sporc.search_episodes(
    category="Science Fiction",
    min_duration=1800
)
print(f"Found {len(science_fiction_episodes)} science fiction episodes")

# Find religion and spirituality episodes
religion_episodes = sporc.search_episodes(
    category="Religion & Spirituality",
    min_duration=1800
)
print(f"Found {len(religion_episodes)} religion and spirituality episodes")

# Find specific religion subcategories
christianity_episodes = sporc.search_episodes(
    category="Christianity",
    min_duration=1800
)
print(f"Found {len(christianity_episodes)} Christianity episodes")

buddhism_episodes = sporc.search_episodes(
    category="Buddhism",
    min_duration=1800
)
print(f"Found {len(buddhism_episodes)} Buddhism episodes")

# Find kids and family episodes
kids_episodes = sporc.search_episodes(
    category="Kids & Family",
    max_duration=1800                 # Shorter episodes for kids
)
print(f"Found {len(kids_episodes)} kids and family episodes")

# Find specific kids subcategories
parenting_episodes = sporc.search_episodes(
    category="Parenting",
    min_duration=900                  # 15+ minutes
)
print(f"Found {len(parenting_episodes)} parenting episodes")

stories_for_kids_episodes = sporc.search_episodes(
    category="Stories for Kids",
    max_duration=1200                 # 20 minutes or less
)
print(f"Found {len(stories_for_kids_episodes)} stories for kids episodes")
```

## Performance-Optimized Searches

### Efficient Large-Scale Searches

```python
# Use streaming mode for large searches
sporc_streaming = SPORCDataset(streaming=True)

# Search efficiently in streaming mode
long_episodes = []
for episode in sporc_streaming.iterate_episodes():
    if (episode.duration_seconds >= 3600 and
        episode.speaker_count >= 2 and
        episode.speaker_count <= 4):
        long_episodes.append(episode)

    if len(long_episodes) >= 100:  # Limit results
        break

print(f"Found {len(long_episodes)} long episodes with 2-4 speakers")
```

### Selective Loading for Focused Analysis

```python
# Load specific subset for fast access
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(
    categories=['Education', 'Science'],
    min_episodes=5,
    min_total_duration=2.0
)

# Now fast searches within the subset
long_education = sporc.search_episodes(min_duration=1800)
multi_speaker_science = sporc.search_episodes(
    category="Science",
    min_speakers=3
)

print(f"Found {len(long_education)} long education episodes")
print(f"Found {len(multi_speaker_science)} multi-speaker science episodes")
```

## Search Result Analysis

### Analyzing Search Results

```python
# Perform a complex search
results = sporc.search_episodes(
    category="Education",
    min_duration=1800,
    min_speakers=2,
    max_speakers=4,
    language="en"
)

print(f"Found {len(results)} matching episodes")

# Analyze the results
if results:
    durations = [ep.duration_seconds for ep in results]
    speaker_counts = [ep.speaker_count for ep in results]

    print(f"Average duration: {sum(durations) / len(durations) / 60:.1f} minutes")
    print(f"Average speaker count: {sum(speaker_counts) / len(speaker_counts):.1f}")

    # Group by podcast
    podcasts = {}
    for episode in results:
        podcast_title = episode.podcast.title
        if podcast_title not in podcasts:
            podcasts[podcast_title] = []
        podcasts[podcast_title].append(episode)

    print(f"Episodes from {len(podcasts)} different podcasts")

    # Show top podcasts
    top_podcasts = sorted(podcasts.items(), key=lambda x: len(x[1]), reverse=True)[:5]
    for podcast_title, episodes in top_podcasts:
        print(f"  {podcast_title}: {len(episodes)} episodes")
```

### Filtering and Sorting Results

```python
# Get search results
episodes = sporc.search_episodes(category="Education", min_duration=1800)

# Sort by duration (longest first)
longest_first = sorted(episodes, key=lambda ep: ep.duration_seconds, reverse=True)
print("Longest education episodes:")
for ep in longest_first[:5]:
    print(f"  {ep.title}: {ep.duration_minutes:.1f} minutes")

# Sort by speaker count
most_speakers = sorted(episodes, key=lambda ep: ep.speaker_count, reverse=True)
print("Education episodes with most speakers:")
for ep in most_speakers[:5]:
    print(f"  {ep.title}: {ep.speaker_count} speakers")

# Filter by quality
high_quality = [ep for ep in episodes if ep.overlap_prop_duration < 0.1]
print(f"High-quality episodes: {len(high_quality)} out of {len(episodes)}")
```

## Error Handling in Searches

```python
from sporc import SPORCDataset, SPORCError

try:
    sporc = SPORCDataset()

    # Perform search
    results = sporc.search_episodes(
        category="Education",
        min_duration=1800
    )

    print(f"Search successful: {len(results)} results")

except SPORCError as e:
    print(f"Search error: {e}")
except ValueError as e:
    print(f"Invalid search parameters: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

## Tips for Effective Searching

1. **Start Simple**: Begin with basic criteria and add complexity gradually
2. **Use Selective Loading**: For focused analysis, load specific subsets
3. **Consider Quality**: Include quality filters for research applications
4. **Limit Results**: Use result limits for large datasets
5. **Cache Results**: Store search results for repeated analysis
6. **Validate Parameters**: Ensure search parameters are within valid ranges
7. **Use Streaming**: For large-scale searches, use streaming mode
8. **Combine Criteria**: Use multiple criteria for more precise results
9. **Use Subcategories**: Leverage specific subcategories for more targeted searches
10. **Check Category Validity**: Use the category utility functions to validate category names