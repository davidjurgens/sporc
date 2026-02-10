"""
Tests for IndexNotBuiltError exception hierarchy and behavior.
"""

import pytest

from sporc.exceptions import SPORCError, IndexNotBuiltError


class TestIndexNotBuiltError:
    """Tests for IndexNotBuiltError."""

    def test_subclass_of_sporc_error(self):
        assert issubclass(IndexNotBuiltError, SPORCError)

    def test_subclass_of_exception(self):
        assert issubclass(IndexNotBuiltError, Exception)

    def test_catchable_as_sporc_error(self):
        with pytest.raises(SPORCError):
            raise IndexNotBuiltError("index missing")

    def test_message_preserved(self):
        err = IndexNotBuiltError("Speaker index not found")
        assert str(err) == "Speaker index not found"

    def test_empty_message(self):
        err = IndexNotBuiltError()
        assert str(err) == ""
