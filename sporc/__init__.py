"""
SPORC: Structured Podcast Open Research Corpus

A Python package for working with the SPORC dataset from Hugging Face.
"""

from .dataset import SPORCDataset
from .podcast import Podcast
from .episode import Episode
from .turn import Turn
from .exceptions import SPORCError

__version__ = "0.1.0"
__author__ = "SPORC Package Maintainer"
__email__ = "maintainer@example.com"

__all__ = [
    "SPORCDataset",
    "Podcast",
    "Episode",
    "Turn",
    "SPORCError",
]