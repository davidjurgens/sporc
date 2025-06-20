"""
SPORC: Structured Podcast Open Research Corpus

A Python package for working with the SPORC dataset from Hugging Face.
"""

from .dataset import SPORCDataset
from .podcast import Podcast
from .episode import Episode, TimeRangeBehavior
from .turn import Turn
from .exceptions import SPORCError, DatasetAccessError, AuthenticationError, NotFoundError
from .constants import (
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

__version__ = "0.1.0"
__author__ = "SPORC Package Maintainer"
__email__ = "maintainer@example.com"

__all__ = [
    "SPORCDataset",
    "Podcast",
    "Episode",
    "Turn",
    "TimeRangeBehavior",
    "SPORCError",
    "DatasetAccessError",
    "AuthenticationError",
    "NotFoundError",
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