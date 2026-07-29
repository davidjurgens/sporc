"""
SPORC: Structured Podcast Open Research Corpus

A Python package for working with the SPORC dataset from Hugging Face.
"""

from .dataset import SPORCDataset
from .podcast import Podcast
from .episode import Episode, TimeRangeBehavior, TurnWindow
from .turn import Turn
from .exceptions import (
    SPORCError,
    DatasetAccessError,
    AuthenticationError,
    IndexNotBuiltError,
    NotFoundError,
    DataNotLocalError,
    FrameTooLargeError,
)
from .source import DataSource, LocalDataSource, HubDataSource
# The columnar API. These do not import pandas at module level -- it is pulled
# in inside the functions -- so `import sporc` stays cheap.
from .frames import (
    add_speaker_columns,
    describe_columns,
    list_catalogs,
    list_tables,
    load_catalog,
    parse_episode_dates,
    window_frame_from_turns,
)
from .constants import (
    NO_INFERRED_SPEAKER,
    NO_INFERRED_ROLE,
    SPEAKER_UNKNOWN,
    PLACEHOLDER_SPEAKERS,
    ANON_SPEAKER_RE,
    is_placeholder_speaker,
    APPLE_PODCAST_CATEGORIES,
    ALL_CATEGORIES,
    MAIN_CATEGORIES,
    SUBCATEGORIES,
    CATEGORY_HIERARCHY,
    SUBCATEGORY_TO_MAIN,
    QUALITY_THRESHOLDS,
    SUPPORTED_LANGUAGES,
    LANGUAGE_CODES,
    LANGUAGE_NAMES,
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
)

__version__ = "1.2.0"
__author__ = "David Jurgens"
__email__ = "jurgens@umich.edu"

__all__ = [
    "SPORCDataset",
    "Podcast",
    "Episode",
    "Turn",
    "TurnWindow",
    "TimeRangeBehavior",
    "SPORCError",
    "DatasetAccessError",
    "AuthenticationError",
    "IndexNotBuiltError",
    "NotFoundError",
    "DataNotLocalError",
    "FrameTooLargeError",
    # Data sources
    "DataSource",
    "LocalDataSource",
    "HubDataSource",
    # Columnar API. The dataset methods (turns_frame, window_frame, catalog,
    # columns, parquet_paths) live on SPORCDataset; these are the pieces that
    # work without one.
    "load_catalog",
    "list_catalogs",
    "describe_columns",
    "list_tables",
    "add_speaker_columns",
    "parse_episode_dates",
    "window_frame_from_turns",
    # Speaker sentinels. The inferred_speaker_* columns use these rather than
    # null, so filtering them out is a correctness step, not a nicety.
    "NO_INFERRED_SPEAKER",
    "NO_INFERRED_ROLE",
    "SPEAKER_UNKNOWN",
    "PLACEHOLDER_SPEAKERS",
    "ANON_SPEAKER_RE",
    "is_placeholder_speaker",
    # Constants
    "APPLE_PODCAST_CATEGORIES",
    "ALL_CATEGORIES",
    "MAIN_CATEGORIES",
    "SUBCATEGORIES",
    "CATEGORY_HIERARCHY",
    "SUBCATEGORY_TO_MAIN",
    "QUALITY_THRESHOLDS",
    "SUPPORTED_LANGUAGES",
    "LANGUAGE_CODES",
    "LANGUAGE_NAMES",
    # Utility functions
    "get_main_category",
    "get_subcategories",
    "is_main_category",
    "is_subcategory",
    "is_valid_category",
    "get_all_categories",
    "get_main_categories",
    "get_subcategories_list",
    "get_subcategories_by_main_category",
    "get_subcategories_with_episodes",
    "get_subcategory_statistics",
    "search_subcategories",
    "get_popular_subcategories",
]