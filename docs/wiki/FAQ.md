# Frequently Asked Questions (FAQ)

This page answers common questions about the SPORC package and provides solutions to frequently encountered issues.

## Installation and Setup

### Q: How do I install the SPORC package?

**A:** Install the package using pip:

```bash
pip install sporc
```

Make sure you have:
1. Accepted the dataset terms on [Hugging Face](https://huggingface.co/datasets/blitt/SPoRC)
2. Set up Hugging Face authentication with `huggingface-cli login`

### Q: I get an authentication error when trying to load the dataset. What should I do?

**A:** This usually means you need to authenticate with Hugging Face:

1. Install the Hugging Face Hub: `pip install huggingface_hub`
2. Login: `huggingface-cli login`
3. Enter your token from [https://huggingface.co/settings/tokens](https://huggingface.co/settings/tokens)

### Q: I get a "Dataset not found" error. What's wrong?

**A:** You need to accept the dataset terms first:

1. Visit [https://huggingface.co/datasets/blitt/SPoRC](https://huggingface.co/datasets/blitt/SPoRC)
2. Log in to your Hugging Face account
3. Click "I agree" to accept the dataset terms
4. Try loading the dataset again

### Q: The installation is taking a long time. Is this normal?

**A:** Yes, the first time you load the dataset, it needs to download several GB of data. This can take 10-30 minutes depending on your internet connection. Subsequent loads will be much faster as the data is cached locally.

## Usage

### Q: What's the difference between memory mode and streaming mode?

**A:**
- **Memory mode** (default): Loads the entire dataset into RAM for fast access. Requires 8GB+ RAM but provides instant search and access.
- **Streaming mode**: Loads data on-demand, using much less memory (50-100MB) but slower access. Good for systems with limited RAM.

### Q: How do I search for a specific podcast?

**A:** Use the `search_podcast` method:

```python
from sporc import SPORCDataset

sporc = SPORCDataset()
podcast = sporc.search_podcast("Exact Podcast Name")
if podcast:
    print(f"Found: {podcast.title}")
```

### Q: How do I find episodes longer than 30 minutes?

**A:** Use the `search_episodes` method:

```python
long_episodes = sporc.search_episodes(min_duration=1800)  # 1800 seconds = 30 minutes
print(f"Found {len(long_episodes)} episodes")
```

### Q: How do I get all conversation turns from an episode?

**A:** Use the `get_all_turns` method:

```python
episode = podcast.episodes[0]
turns = episode.get_all_turns()
for turn in turns:
    print(f"{turn.speaker}: {turn.text[:100]}...")
```

### Q: How do I analyze speaker participation?

**A:** Use the `get_turns_by_speaker` method:

```python
for speaker in episode.main_speakers:
    speaker_turns = episode.get_turns_by_speaker(speaker)
    total_time = sum(t.duration for t in speaker_turns)
    print(f"{speaker}: {total_time/60:.1f} minutes")
```

## Categories

### Q: What categories are available in SPORC?

**A:** SPORC uses the official [Apple Podcasts categories](https://podcasters.apple.com/support/1691-apple-podcasts-categories). There are 19 main categories with many subcategories:

**Main Categories:**
- Arts, Business, Comedy, Education, Fiction, Government, History, Health & Fitness, Kids & Family, Leisure, Music, News, Religion & Spirituality, Science, Society & Culture, Sports, Technology, True Crime, TV & Film

**Popular Subcategories:**
- Education: Language Learning, Self-Improvement, Courses, How To
- Business: Entrepreneurship, Investing, Management, Marketing
- Science: Astronomy, Physics, Chemistry, Life Sciences
- News: Politics, Tech News, Business News, Sports News
- Health & Fitness: Mental Health, Nutrition, Medicine, Fitness

### Q: How do I search by category?

**A:** Use the `category` parameter in `search_episodes`:

```python
# Search by main category
education_episodes = sporc.search_episodes(category="Education")

# Search by subcategory
language_learning = sporc.search_episodes(category="Language Learning")

# Search multiple categories
business_science = sporc.search_episodes(category=["Business", "Science"])
```

### Q: How do I check if a category is valid?

**A:** Use the category utility functions:

```python
from sporc import is_valid_category, is_main_category, is_subcategory

print(is_valid_category("Education"))      # True
print(is_valid_category("Invalid"))        # False
print(is_main_category("Education"))       # True
print(is_subcategory("Language Learning")) # True
```

### Q: How do I get all available categories?

**A:** Use the category utility functions:

```python
from sporc import get_all_categories, get_main_categories, get_subcategories

all_categories = get_all_categories()
main_categories = get_main_categories()
science_subcategories = get_subcategories("Science")
```

### Q: Can I search by both main categories and subcategories?

**A:** Yes! You can search by any valid category name, whether it's a main category or subcategory:

```python
# These all work:
episodes = sporc.search_episodes(category="Education")           # Main category
episodes = sporc.search_episodes(category="Language Learning")   # Subcategory
episodes = sporc.search_episodes(category=["Education", "Language Learning"])  # Both
```

### Q: What's the difference between main categories and subcategories?

**A:**
- **Main categories** are the top-level categories (e.g., "Education", "Science", "Business")
- **Subcategories** are more specific classifications within main categories (e.g., "Language Learning" is a subcategory of "Education")
- You can search by either, and the system will find all matching episodes

### Q: How do I search specifically by subcategory?

**A:** You can use dedicated subcategory search methods:

```python
# Use the dedicated subcategory search method
language_episodes = sporc.search_episodes_by_subcategory("Language Learning")

# Use the subcategory parameter in general search
self_improvement_episodes = sporc.search_episodes(subcategory="Self-Improvement")

# Search for podcasts by subcategory
language_podcasts = sporc.search_podcasts_by_subcategory("Language Learning")
```

### Q: How do I find all subcategories for a main category?

**A:** Use the subcategory utility functions:

```python
from sporc import get_subcategories_by_main_category

science_subcategories = get_subcategories_by_main_category("Science")
print(science_subcategories)  # ['Astronomy', 'Chemistry', 'Earth Sciences', ...]
```

### Q: How do I search for subcategories by name?

**A:** Use the search function:

```python
from sporc import search_subcategories

tech_matches = search_subcategories("tech")
print(tech_matches)  # ['Tech News', 'Technology']
```

### Q: How do I get subcategory information for a podcast?

**A:** Use the subcategory properties:

```python
podcast = sporc.search_podcast("Some Podcast")
print(f"Subcategories: {podcast.subcategories}")
print(f"Primary subcategory: {podcast.primary_subcategory}")
```

## Performance and Memory

### Q: The dataset is using too much memory. What can I do?

**A:** Use streaming mode:

```python
sporc = SPORCDataset(streaming=True)
```

Or use selective loading to load only specific podcasts:

```python
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(categories=['Education'])
```

### Q: Search operations are very slow. How can I speed them up?

**A:**
1. Use memory mode instead of streaming mode
2. Use selective loading to work with a smaller subset
3. Cache search results if you're doing repeated searches

### Q: I'm getting a "RuntimeError" when trying to use `len()` on the dataset. Why?

**A:** In streaming mode, you can't use `len()` because the dataset size isn't known until you iterate through all data. Either:
1. Use memory mode: `sporc = SPORCDataset()`
2. Load a subset first: `sporc.load_podcast_subset(...)`
3. Count manually: `count = sum(1 for _ in sporc.iterate_episodes())`

### Q: What does `len(sporc)` return in streaming mode?

**A:** In streaming mode, `len(sporc)` returns 1,134,058 (the total number of episodes) unless a subset has been loaded, in which case it returns the size of the subset.

## Data Quality

### Q: What do the overlap proportion metrics mean?

**A:** These indicate the quality of speaker diarization:
- `overlap_prop_duration`: Proportion of time where multiple speakers are talking simultaneously
- `overlap_prop_turn_count`: Proportion of turns that overlap with other turns
- Lower values (0.05-0.1) indicate better quality
- Higher values (0.2+) indicate poorer quality

### Q: How do I filter for high-quality episodes?

**A:** Use quality filters in your search:

```python
good_episodes = sporc.search_episodes(
    max_overlap_prop_duration=0.1,    # Less than 10% overlap
    max_overlap_prop_turn_count=0.2   # Less than 20% overlapping turns
)
```

### Q: Some episodes have very short turns. Is this normal?

**A:** Yes, this is normal. Podcasts often have:
- Short interjections ("uh-huh", "right")
- Quick responses
- Overlapping speech
- Background noise or music

You can filter for longer turns if needed:

```python
long_turns = episode.get_turns_by_min_length(10)  # 10+ second turns
```

### Q: I get a "JSON parse error: Column changed from string to array" error. What's wrong?

**A:** This error occurs when the Hugging Face dataset contains inconsistent data types for certain fields. The error message shows something like:
```
JSON parse error: Column(/neitherPredictedNames) changed from string to array in row 2
```

**Solution**: This is a data quality issue in the Hugging Face dataset itself. Try these steps:

1. **Clear your cache** (most common solution):
   ```bash
   rm -rf ~/.cache/huggingface/
   ```

2. **Use the cache fix script**:
   ```bash
   python fix_dataset_cache.py
   ```

3. **Try alternative loading methods**:
   - Use memory mode instead of streaming mode
   - Use selective loading to load only specific categories
   - Try loading at a different time (the dataset might be temporarily corrupted)

4. **If the problem persists**:
   - This indicates a deeper issue with the dataset itself
   - Contact the dataset maintainers on Hugging Face
   - Try using an older version of the dataset if available

**Note**: This is not a bug in the SPORC package, but rather a data quality issue in the underlying Hugging Face dataset. The package includes error handling for this situation, but sometimes the dataset itself needs to be fixed by the maintainers.

### Q: I get a "Bad split" error when loading the dataset. What's wrong?

**A:** This error occurs when the dataset structure has changed. The error message shows something like:
```
ValueError: Bad split: episodeLevelDataSample. Available splits: ['train']
```

**Solution**: This has been fixed in the latest version of the SPORC package. The dataset now uses a single 'train' split instead of separate splits for episodes and speaker turns.

If you're still getting this error:
1. Update to the latest version: `pip install --upgrade sporc`
2. Clear your cache: `rm -rf ~/.cache/huggingface/`
3. Try loading the dataset again

If the problem persists, please report it as an issue on GitHub.

## Troubleshooting

### Q: I get an "ImportError: No module named 'datasets'". What should I do?

**A:** Install the required dependencies:

```bash
pip install datasets huggingface_hub
```

Or reinstall the package:

```bash
pip install --force-reinstall sporc
```

### Q: I get a "PermissionError" when trying to access the cache. What's wrong?

**A:** This is usually a file permission issue. Try:

1. Use a virtual environment
2. Install with `--user` flag: `pip install --user sporc`
3. Check permissions on your cache directory
4. Clear the cache: `rm -rf ~/.cache/huggingface/`

### Q: The dataset seems to be corrupted. How do I fix it?

**A:** Clear the cache and redownload:

```bash
rm -rf ~/.cache/huggingface/
```

Then try loading the dataset again.

### Q: I'm getting different results each time I run the same search. Why?

**A:** This shouldn't happen with the same dataset version. Possible causes:
1. You're using streaming mode and the data order is different
2. The dataset was updated
3. You're using different search parameters

Try using memory mode for consistent results.

### Q: My category search returns no results. What's wrong?

**A:** Check the following:

1. **Case sensitivity**: Category names are case-sensitive. Use "Education" not "education"
2. **Exact spelling**: Use exact category names from the Apple Podcasts list
3. **Check validity**: Use `is_valid_category("Your Category")` to verify
4. **Try main categories**: If a subcategory returns no results, try the main category

```python
from sporc import is_valid_category

# Check if your category is valid
if is_valid_category("Your Category"):
    episodes = sporc.search_episodes(category="Your Category")
    print(f"Found {len(episodes)} episodes")
else:
    print("Invalid category name")
```

## Advanced Usage

### Q: How do I analyze conversation patterns across multiple episodes?

**A:** Use selective loading to get a subset, then analyze:

```python
sporc = SPORCDataset(streaming=True)
sporc.load_podcast_subset(categories=['Education'])

all_turns = []
for episode in sporc.get_all_episodes():
    turns = episode.get_all_turns()
    all_turns.extend(turns)

# Analyze all turns
print(f"Total turns: {len(all_turns)}")
```

### Q: How do I export conversation data for analysis in other tools?

**A:** Convert to common formats:

```python
import pandas as pd

# Convert turns to DataFrame
turns_data = []
for episode in sporc.get_all_episodes():
    for turn in episode.get_all_turns():
        turns_data.append({
            'episode_title': episode.title,
            'speaker': turn.speaker,
            'start_time': turn.start_time,
            'end_time': turn.end_time,
            'duration': turn.duration,
            'text': turn.text,
            'word_count': turn.word_count
        })

df = pd.DataFrame(turns_data)
df.to_csv('conversation_data.csv', index=False)
```

### Q: How do I analyze speaker transitions?

**A:** Use the `get_speaker_transitions` method:

```python
transitions = episode.get_speaker_transitions()
from collections import Counter
transition_counts = Counter(transitions)

print("Most common transitions:")
for (speaker1, speaker2), count in transition_counts.most_common(5):
    print(f"{speaker1} → {speaker2}: {count} times")
```

### Q: How do I find episodes with specific hosts?

**A:** Use the `host_name` parameter:

```python
simon_episodes = sporc.search_episodes(host_name="Simon Shapiro")
print(f"Found {len(simon_episodes)} episodes by Simon Shapiro")
```

### Q: How do I analyze conversation quality across the dataset?

**A:** Use streaming mode to process all episodes:

```python
sporc = SPORCDataset(streaming=True)

quality_stats = []
for episode in sporc.iterate_episodes():
    quality_stats.append({
        'title': episode.title,
        'overlap_duration': episode.overlap_prop_duration,
        'overlap_turns': episode.overlap_prop_turn_count,
        'avg_turn_duration': episode.avg_turn_duration
    })

# Analyze quality distribution
import pandas as pd
df = pd.DataFrame(quality_stats)
print(f"Average overlap: {df['overlap_duration'].mean():.3f}")
```

### Q: How do I compare different categories?

**A:** Use category-based analysis:

```python
def compare_categories(category1, category2):
    episodes1 = sporc.search_episodes(category=category1)
    episodes2 = sporc.search_episodes(category=category2)

    print(f"{category1}: {len(episodes1)} episodes")
    print(f"{category2}: {len(episodes2)} episodes")

    if episodes1 and episodes2:
        avg_duration1 = sum(ep.duration_minutes for ep in episodes1) / len(episodes1)
        avg_duration2 = sum(ep.duration_minutes for ep in episodes2) / len(episodes2)
        print(f"Avg duration: {avg_duration1:.1f} vs {avg_duration2:.1f} minutes")

compare_categories("Education", "Science")
```

## Research Applications

### Q: How do I use SPORC for conversation analysis research?

**A:** Start with the [Conversation Analysis](Conversation-Analysis.md) guide. Key approaches:

1. Use `episode.get_turns_by_time_range()` for time-based analysis
2. Use `episode.get_turns_by_speaker()` for speaker-specific analysis
3. Use `episode.get_all_turns()` for comprehensive analysis
4. Analyze turn-taking patterns with `episode.get_speaker_transitions()`
5. Get conversation flow with `episode.get_conversation_flow()`

For more advanced analysis, see the [Conversation Analysis](Conversation-Analysis.md) guide.

### Q: How do I compare conversation patterns across different podcast categories?

**A:** Use selective loading for different categories:

```python
# Load education podcasts
sporc_edu = SPORCDataset(streaming=True)
sporc_edu.load_podcast_subset(categories=['Education'])

# Load science podcasts
sporc_sci = SPORCDataset(streaming=True)
sporc_sci.load_podcast_subset(categories=['Science'])

# Compare patterns
edu_turns = sum(len(ep.get_all_turns()) for ep in sporc_edu.get_all_episodes())
sci_turns = sum(len(ep.get_all_turns()) for ep in sporc_sci.get_all_episodes())

print(f"Education: {edu_turns} turns")
print(f"Science: {sci_turns} turns")
```

### Q: How do I extract features for machine learning?

**A:** Convert conversation data to features:

```python
def extract_features(episode):
    turns = episode.get_all_turns()
    return {
        'turn_count': len(turns),
        'avg_turn_duration': sum(t.duration for t in turns) / len(turns),
        'speaker_count': episode.speaker_count,
        'overlap_prop': episode.overlap_prop_duration,
        'word_count': episode.word_count,
        'words_per_minute': episode.word_count / episode.duration_minutes
    }

features = [extract_features(ep) for ep in sporc.get_all_episodes()]
```

### Q: How do I analyze content by category for research?

**A:** Use category-based content analysis:

```python
def analyze_category_content(category_name):
    episodes = sporc.search_episodes(category=category_name)

    if not episodes:
        return None

    total_episodes = len(episodes)
    total_duration = sum(ep.duration_hours for ep in episodes)
    avg_duration = total_duration / total_episodes

    return {
        'category': category_name,
        'total_episodes': total_episodes,
        'total_duration_hours': total_duration,
        'avg_duration_hours': avg_duration
    }

# Analyze multiple categories
categories = ["Education", "Science", "Business", "News"]
for category in categories:
    analysis = analyze_category_content(category)
    if analysis:
        print(f"{analysis['category']}: {analysis['total_episodes']} episodes")
```

## Getting Help

### Q: Where can I get more help?

**A:**
1. Check the [documentation](Home.md) for detailed guides
2. Review the [Categories](Categories.md) guide for category-specific help
3. Search existing [issues](https://github.com/yourusername/sporc/issues)
4. Check the [FAQ](FAQ.md) page for common solutions
5. Review the [Streaming Mode](Streaming-Mode.md) for optimization advice

If you're still having issues, please:

1. Check that you've accepted the dataset terms on Hugging Face
2. Verify your Hugging Face token is valid
3. Try using streaming mode if you have memory issues
4. Check the [Installation](Installation.md) guide for setup issues

See the [Contributing](Contributing.md) guide for more details.

### Q: How do I load SPORC from a specific cache directory where it's already been downloaded?

**A:** You can use the `custom_cache_dir` parameter to load SPORC from a pre-existing cache location:

```python
from sporc import SPORCDataset

# Load from a specific cache directory
sporc = SPORCDataset(custom_cache_dir='/path/to/your/cache/directory')
```

**Finding your cache directory:**
```bash
python find_sporc_cache.py
```

This script will:
- Search for existing Hugging Face cache directories
- Check if SPORC is already downloaded
- Provide usage instructions for your specific setup

**Alternative methods:**

1. **Use `cache_dir` parameter:**
   ```python
   sporc = SPORCDataset(cache_dir='/path/to/cache')
   ```

2. **Set environment variable:**
   ```bash
   export HF_HOME='/path/to/cache'
   ```
   ```python
   sporc = SPORCDataset()  # Will use HF_HOME
   ```

**Cache directory differences:**
- `custom_cache_dir`: Loads from specific directory, downloads there if needed
- `cache_dir`: Uses as cache location, may download if not found
- `HF_HOME`: Sets default Hugging Face cache location

**Common cache locations:**
- Linux/macOS: `~/.cache/huggingface/`
- macOS: `~/Library/Caches/huggingface/`
- Windows: `~/AppData/Local/huggingface/`