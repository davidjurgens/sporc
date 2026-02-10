"""
Tests for build_indexes.py helper functions and regex patterns.
"""

import os
import sys
import pytest

# scripts/ is not a package, so add it to sys.path
_scripts_dir = os.path.join(os.path.dirname(__file__), "..", "scripts")
sys.path.insert(0, os.path.abspath(_scripts_dir))

from build_indexes import DISCOURSE_MARKERS, _count_discourse_markers, _count_words


# ===================================================================
# DISCOURSE_MARKERS regex
# ===================================================================


class TestDiscourseMarkersRegex:
    """Tests for the DISCOURSE_MARKERS compiled regex."""

    @pytest.mark.parametrize(
        "marker",
        ["um", "uh", "uh huh", "mm hmm", "like", "you know",
         "i mean", "so", "well", "right", "okay", "oh"],
    )
    def test_each_marker_matches(self, marker):
        matches = DISCOURSE_MARKERS.findall(marker)
        assert len(matches) >= 1

    @pytest.mark.parametrize(
        "marker",
        ["Um", "UH", "Like", "YOU KNOW", "I Mean", "OKAY"],
    )
    def test_case_insensitive(self, marker):
        matches = DISCOURSE_MARKERS.findall(marker)
        assert len(matches) >= 1

    def test_word_boundary_no_false_positive_some(self):
        """'some' should NOT match 'so'."""
        matches = DISCOURSE_MARKERS.findall("some things are good")
        # "some" should not match "so"
        assert "so" not in [m.lower() for m in matches]

    def test_word_boundary_no_false_positive_likelihood(self):
        """'likelihood' should NOT match 'like'."""
        matches = DISCOURSE_MARKERS.findall("the likelihood is high")
        assert "like" not in [m.lower() for m in matches]

    def test_multiple_markers_in_text(self):
        text = "um well you know I think like that's right"
        matches = DISCOURSE_MARKERS.findall(text)
        matched_lower = [m.lower() for m in matches]
        assert "um" in matched_lower
        assert "well" in matched_lower
        assert "you know" in matched_lower
        assert "like" in matched_lower
        assert "right" in matched_lower

    def test_no_markers_empty_list(self):
        text = "the cat sat on the mat"
        matches = DISCOURSE_MARKERS.findall(text)
        assert len(matches) == 0


# ===================================================================
# _count_discourse_markers
# ===================================================================


class TestCountDiscourseMarkers:
    """Tests for _count_discourse_markers function."""

    def test_empty_string(self):
        assert _count_discourse_markers("") == 0

    def test_no_markers(self):
        assert _count_discourse_markers("the cat sat on the mat") == 0

    def test_single_marker(self):
        assert _count_discourse_markers("I was um thinking about it") == 1

    def test_multiple_markers(self):
        assert _count_discourse_markers("um well I think so") >= 3

    def test_repeated_markers(self):
        assert _count_discourse_markers("um um um") == 3

    def test_case_insensitive_counting(self):
        assert _count_discourse_markers("Um UM um") == 3

    def test_marker_at_boundaries(self):
        assert _count_discourse_markers("um at the start") >= 1
        assert _count_discourse_markers("at the end um") >= 1

    def test_multi_word_marker(self):
        count = _count_discourse_markers("uh huh that makes sense")
        assert count >= 1


# ===================================================================
# _count_words
# ===================================================================


class TestCountWords:
    """Tests for _count_words function."""

    def test_empty_string(self):
        assert _count_words("") == 0

    def test_single_word(self):
        assert _count_words("hello") == 1

    def test_multiple_words(self):
        assert _count_words("hello beautiful world") == 3

    def test_extra_whitespace(self):
        assert _count_words("hello   world") == 2

    def test_leading_trailing_whitespace(self):
        assert _count_words("  hello world  ") == 2

    def test_tabs_and_newlines(self):
        assert _count_words("hello\tworld\nfoo") == 3

    def test_only_whitespace(self):
        assert _count_words("   ") == 0
