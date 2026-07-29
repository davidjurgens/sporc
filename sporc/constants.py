"""
Constants for the SPORC package.

This module contains constants used throughout the SPORC package,
including Apple Podcasts categories and other configuration values.
"""

import re
from typing import Any, Dict, List, Optional, Set

# ---------------------------------------------------------------------------
# Speaker sentinels
# ---------------------------------------------------------------------------
# These live here rather than in sporc/phonetics.py, where they started, because
# phonetics pulls in torch and Praat and is deliberately imported on demand.
# A constant nobody can import without paying for the alignment stack is a
# constant nobody imports.

#: Written into ``inferred_speaker_name`` when name attribution found nobody.
NO_INFERRED_SPEAKER = "NO_INFERRED_SPEAKER"

#: Written into ``inferred_speaker_role`` when role attribution found nothing.
NO_INFERRED_ROLE = "NO_INFERRED_ROLE"

#: Written where a speaker is known to exist but could not be identified.
SPEAKER_UNKNOWN = "SPEAKER_UNKNOWN"

#: Speaker values that name no one.
#:
#: The two ``inferred_speaker_*`` columns never contain nulls. They contain
#: these sentinels, and the sentinel is the overwhelming majority: 81% of turns
#: in the tutorial subset, 89.6% in the news genre slice. Two things follow, and
#: both are easy to hit:
#:
#: - ``dropna()`` removes nothing and ``WHERE x IS NOT NULL`` keeps every
#:   placeholder row, so any per-speaker grouping silently pools hundreds of
#:   different people into one "speaker".
#: - The name is guessable and the guess is wrong. Filtering on
#:   ``NO_INFERRED_NAME`` -- a value that does not occur -- is a filter that
#:   never fires, and the output looks entirely reasonable.
#:
#: Use this set, or :func:`is_placeholder_speaker`, rather than a literal.
PLACEHOLDER_SPEAKERS = frozenset({
    NO_INFERRED_SPEAKER,
    NO_INFERRED_ROLE,
    SPEAKER_UNKNOWN,
})

#: Raw diarization labels (``SPEAKER_00``, ...). These are per-episode:
#: ``SPEAKER_00`` in one episode is not ``SPEAKER_00`` in the next, so they
#: cannot identify a person across a podcast the way an inferred name can.
#: Counting episodes per label without accounting for this is how a speaker
#: appears to span the whole corpus.
ANON_SPEAKER_RE = re.compile(r"^SPEAKER_\d+$", re.I)


def is_placeholder_speaker(value: Optional[str]) -> bool:
    """
    Whether *value* names no one.

    True for null, for the empty string, for every member of
    :data:`PLACEHOLDER_SPEAKERS`, and for a raw per-episode diarization label
    such as ``SPEAKER_00`` -- which names a voice within one episode but not a
    person, and so cannot be grouped on across episodes.
    """
    if not value:
        return True
    return value in PLACEHOLDER_SPEAKERS or bool(ANON_SPEAKER_RE.match(value))


# Apple Podcasts Categories
# Source: https://podcasters.apple.com/support/1691-apple-podcasts-categories

APPLE_PODCAST_CATEGORIES = {
    "Arts": [
        "Books",
        "Design",
        "Fashion & Beauty",
        "Food",
        "Performing Arts",
        "Visual Arts"
    ],
    "Business": [
        "Careers",
        "Entrepreneurship",
        "Investing",
        "Management",
        "Marketing",
        "Non-Profit"
    ],
    "Comedy": [
        "Comedy Interviews",
        "Improv",
        "Stand-Up"
    ],
    "Education": [
        "Courses",
        "How To",
        "Language Learning",
        "Self-Improvement"
    ],
    "Fiction": [
        "Comedy Fiction",
        "Drama",
        "Science Fiction"
    ],
    "Government": [],
    "History": [],
    "Health & Fitness": [
        "Alternative Health",
        "Fitness",
        "Medicine",
        "Mental Health",
        "Nutrition",
        "Sexuality"
    ],
    "Kids & Family": [
        "Education for Kids",
        "Parenting",
        "Pets & Animals",
        "Stories for Kids"
    ],
    "Leisure": [
        "Animation & Manga",
        "Automotive",
        "Aviation",
        "Crafts",
        "Games",
        "Hobbies",
        "Home & Garden",
        "Video Games"
    ],
    "Music": [
        "Music Commentary",
        "Music History",
        "Music Interviews"
    ],
    "News": [
        "Business News",
        "Daily News",
        "Entertainment News",
        "News Commentary",
        "Politics",
        "Sports News",
        "Tech News"
    ],
    "Religion & Spirituality": [
        "Buddhism",
        "Christianity",
        "Hinduism",
        "Islam",
        "Judaism",
        "Religion",
        "Spirituality"
    ],
    "Science": [
        "Astronomy",
        "Chemistry",
        "Earth Sciences",
        "Life Sciences",
        "Mathematics",
        "Natural Sciences",
        "Nature",
        "Physics",
        "Social Sciences"
    ],
    "Society & Culture": [
        "Documentary",
        "Personal Journals",
        "Philosophy",
        "Places & Travel",
        "Relationships"
    ],
    "Sports": [
        "Baseball",
        "Basketball",
        "Cricket",
        "Fantasy Sports",
        "Football",
        "Golf",
        "Hockey",
        "Rugby",
        "Running",
        "Soccer",
        "Swimming",
        "Tennis",
        "Volleyball",
        "Wilderness",
        "Wrestling"
    ],
    "Technology": [],
    "True Crime": [],
    "TV & Film": [
        "After Shows",
        "Film History",
        "Film Interviews",
        "Film Reviews",
        "TV Reviews"
    ]
}

# All categories (main categories + subcategories)
ALL_CATEGORIES: Set[str] = set()
for main_category, subcategories in APPLE_PODCAST_CATEGORIES.items():
    ALL_CATEGORIES.add(main_category)
    ALL_CATEGORIES.update(subcategories)

# Main categories only
MAIN_CATEGORIES: Set[str] = set(APPLE_PODCAST_CATEGORIES.keys())

# Subcategories only
SUBCATEGORIES: Set[str] = set()
for subcategories in APPLE_PODCAST_CATEGORIES.values():
    SUBCATEGORIES.update(subcategories)

# Category hierarchy (main category -> subcategories)
CATEGORY_HIERARCHY: Dict[str, List[str]] = APPLE_PODCAST_CATEGORIES

# Reverse lookup (subcategory -> main category)
SUBCATEGORY_TO_MAIN: Dict[str, str] = {}
for main_category, subcategories in APPLE_PODCAST_CATEGORIES.items():
    for subcategory in subcategories:
        SUBCATEGORY_TO_MAIN[subcategory] = main_category

# Quality thresholds for conversation analysis
QUALITY_THRESHOLDS = {
    "EXCELLENT_OVERLAP": 0.05,  # 5% overlap
    "GOOD_OVERLAP": 0.1,        # 10% overlap
    "MODERATE_OVERLAP": 0.2,    # 20% overlap
    "POOR_OVERLAP": 0.3,        # 30% overlap
}

# Supported languages
SUPPORTED_LANGUAGES = {
    "en": "English",
    "es": "Spanish",
    "fr": "French",
    "de": "German",
    "it": "Italian",
    "pt": "Portuguese",
    "nl": "Dutch",
    "sv": "Swedish",
    "no": "Norwegian",
    "da": "Danish",
    "ja": "Japanese",
    "ko": "Korean",
    "zh": "Chinese",
    "ru": "Russian",
    "ar": "Arabic",
    "hi": "Hindi",
    "tr": "Turkish",
    "pl": "Polish",
    "cs": "Czech",
    "hu": "Hungarian",
    "fi": "Finnish",
    "el": "Greek",
    "he": "Hebrew",
    "th": "Thai",
    "vi": "Vietnamese",
    "id": "Indonesian",
    "ms": "Malay",
    "fa": "Persian",
    "ur": "Urdu",
    "bn": "Bengali",
    "ta": "Tamil",
    "te": "Telugu",
    "ml": "Malayalam",
    "kn": "Kannada",
    "gu": "Gujarati",
    "pa": "Punjabi",
    "or": "Odia",
    "as": "Assamese",
    "ne": "Nepali",
    "si": "Sinhala",
    "my": "Burmese",
    "km": "Khmer",
    "lo": "Lao",
    "mn": "Mongolian",
    "ka": "Georgian",
    "am": "Amharic",
    "sw": "Swahili",
    "zu": "Zulu",
    "af": "Afrikaans",
    "xh": "Xhosa",
    "yo": "Yoruba",
    "ig": "Igbo",
    "ha": "Hausa",
    "so": "Somali",
    "rw": "Kinyarwanda",
    "lg": "Luganda",
    "ak": "Akan",
    "tw": "Twi",
    "ee": "Ewe",
    "fon": "Fon",
    "bam": "Bambara",
    "wol": "Wolof",
    "ful": "Fula",
    "zul": "Zulu",
    "xho": "Xhosa",
    "afr": "Afrikaans",
    "nbl": "Southern Ndebele",
    "nso": "Northern Sotho",
    "sot": "Southern Sotho",
    "tsn": "Tswana",
    "tso": "Tsonga",
    "ven": "Venda",
    "ssw": "Swati",
    "nno": "Norwegian Nynorsk",
    "nob": "Norwegian Bokmål",
    "sme": "Northern Sami",
    "smj": "Lule Sami",
    "sma": "Southern Sami",
    "smn": "Inari Sami",
    "sms": "Skolt Sami",
    "cnr": "Montenegrin",
    "srp": "Serbian",
    "hrv": "Croatian",
    "bos": "Bosnian",
    "slv": "Slovenian",
    "mkd": "Macedonian",
    "bul": "Bulgarian",
    "ron": "Romanian",
    "mol": "Moldovan",
    "alb": "Albanian",
    "kat": "Georgian",
    "arm": "Armenian",
    "aze": "Azerbaijani",
    "kaz": "Kazakh",
    "kir": "Kyrgyz",
    "uzb": "Uzbek",
    "tuk": "Turkmen",
    "taj": "Tajik",
    "mon": "Mongolian",
    "tib": "Tibetan",
    "nep": "Nepali",
    "ben": "Bengali",
    "asm": "Assamese",
    "ori": "Odia",
    "mar": "Marathi",
    "guj": "Gujarati",
    "pan": "Punjabi",
    "kan": "Kannada",
    "mal": "Malayalam",
    "tel": "Telugu",
    "tam": "Tamil",
    "sin": "Sinhala",
    "mya": "Burmese",
    "khm": "Khmer",
    "lao": "Lao",
    "tha": "Thai",
    "vie": "Vietnamese",
    "ind": "Indonesian",
    "msa": "Malay",
    "fil": "Filipino",
    "jav": "Javanese",
    "sun": "Sundanese",
    "mad": "Madurese",
    "min": "Minangkabau",
    "ace": "Acehnese",
    "ban": "Balinese",
    "bug": "Buginese",
    "mak": "Makassarese",
    "sas": "Sasak",
    "tet": "Tetum",
    "tim": "Timor",
    "bik": "Bikol",
    "ceb": "Cebuano",
    "hil": "Hiligaynon",
    "ilo": "Ilocano",
    "pam": "Kapampangan",
    "pag": "Pangasinan",
    "war": "Waray",
    "tgl": "Tagalog",
    "bis": "Bislama",
    "fij": "Fijian",
    "hif": "Fiji Hindi",
    "ton": "Tongan",
    "smo": "Samoan",
    "tah": "Tahitian",
    "haw": "Hawaiian",
    "mao": "Maori",
    "rar": "Rarotongan",
    "niu": "Niuean",
    "tkl": "Tokelauan",
    "tuv": "Tuvaluan",
    "gil": "Gilbertese",
    "mri": "Maori",
}

# Language codes to language names
LANGUAGE_CODES = {code: name for code, name in SUPPORTED_LANGUAGES.items()}

# Language names to language codes
LANGUAGE_NAMES = {name: code for code, name in SUPPORTED_LANGUAGES.items()}

def get_main_category(subcategory: str) -> str:
    """
    Get the main category for a given subcategory.

    Args:
        subcategory: The subcategory name

    Returns:
        The main category name, or the subcategory itself if it's a main category
    """
    return SUBCATEGORY_TO_MAIN.get(subcategory, subcategory)

def get_subcategories(main_category: str) -> List[str]:
    """
    Get all subcategories for a given main category.

    Args:
        main_category: The main category name

    Returns:
        List of subcategory names
    """
    return APPLE_PODCAST_CATEGORIES.get(main_category, [])

def is_main_category(category: str) -> bool:
    """
    Check if a category is a main category.

    Args:
        category: The category name

    Returns:
        True if it's a main category, False otherwise
    """
    return category in MAIN_CATEGORIES

def is_subcategory(category: str) -> bool:
    """
    Check if a category is a subcategory.

    Args:
        category: The category name

    Returns:
        True if it's a subcategory, False otherwise
    """
    return category in SUBCATEGORIES

def is_valid_category(category: str) -> bool:
    """
    Check if a category is valid (either main category or subcategory).

    Args:
        category: The category name

    Returns:
        True if it's a valid category, False otherwise
    """
    return category in ALL_CATEGORIES

def get_all_categories() -> List[str]:
    """
    Get all valid categories (main categories and subcategories).

    Returns:
        List of all category names
    """
    return sorted(list(ALL_CATEGORIES))

def get_main_categories() -> List[str]:
    """
    Get all main categories.

    Returns:
        List of main category names
    """
    return sorted(list(MAIN_CATEGORIES))

def get_subcategories_list() -> List[str]:
    """
    Get all subcategories.

    Returns:
        List of subcategory names
    """
    return sorted(list(SUBCATEGORIES))

def get_subcategories_by_main_category(main_category: str) -> List[str]:
    """
    Get all subcategories for a given main category.

    Args:
        main_category: The main category name

    Returns:
        List of subcategory names
    """
    return APPLE_PODCAST_CATEGORIES.get(main_category, [])

def get_subcategories_with_episodes(subcategory: str) -> List[str]:
    """
    Get all subcategories that are related to a given subcategory.
    This is useful for finding similar subcategories within the same main category.

    Args:
        subcategory: The subcategory name

    Returns:
        List of related subcategory names (same main category)
    """
    main_category = get_main_category(subcategory)
    return get_subcategories(main_category)

def get_subcategory_statistics() -> Dict[str, Any]:
    """
    Get statistics about subcategories.

    Returns:
        Dictionary containing subcategory statistics
    """
    subcategory_counts = {}
    for main_category, subcategories in APPLE_PODCAST_CATEGORIES.items():
        for subcategory in subcategories:
            subcategory_counts[subcategory] = {
                'main_category': main_category,
                'subcategory': subcategory
            }

    return {
        'total_subcategories': len(SUBCATEGORIES),
        'subcategories_by_main_category': {
            main_category: len(subcategories)
            for main_category, subcategories in APPLE_PODCAST_CATEGORIES.items()
        },
        'subcategory_details': subcategory_counts
    }

def search_subcategories(query: str) -> List[str]:
    """
    Search for subcategories by name (case-insensitive partial match).

    Args:
        query: Search query

    Returns:
        List of matching subcategory names
    """
    query_lower = query.lower()
    matches = []

    for subcategory in SUBCATEGORIES:
        if query_lower in subcategory.lower():
            matches.append(subcategory)

    return sorted(matches)

def get_popular_subcategories() -> List[str]:
    """
    Get a list of commonly used subcategories.

    Returns:
        List of popular subcategory names
    """
    return [
        "Language Learning",
        "Self-Improvement",
        "Entrepreneurship",
        "Investing",
        "Astronomy",
        "Physics",
        "Mental Health",
        "Nutrition",
        "Tech News",
        "Politics",
        "Fitness",
        "Medicine",
        "Parenting",
        "Video Games",
        "Automotive",
        "Football",
        "Basketball",
        "Christianity",
        "Buddhism",
        "Philosophy"
    ]