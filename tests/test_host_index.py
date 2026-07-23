"""Host-name indexes: podcast- and episode-grained lookups from the catalog.

Exercises a real Parquet layout via the tmp_parquet_layout fixture (which ships
metadata/host_index.parquet and metadata/host_episode_index.parquet) rather than
a mock, so the lazy load and dataframe filtering are covered end to end.
"""
import os
from unittest.mock import patch

import pytest

from sporc.parquet_backend import ParquetBackend
from sporc.exceptions import IndexNotBuiltError
from conftest import PID_WITH_TURNS, EID_WITH_TURNS, EID_NO_TURNS


class TestHostIndex:
    def test_podcast_lookup_substring(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)
        assert backend.get_podcasts_by_host("ira") == [PID_WITH_TURNS]

    def test_podcast_lookup_is_case_insensitive(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)
        assert backend.get_podcasts_by_host("IRA GLASS") == [PID_WITH_TURNS]

    def test_exact_rejects_substring(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)
        assert backend.get_podcasts_by_host("ira", exact=True) == []
        assert backend.get_podcasts_by_host("Ira Glass", exact=True) == [
            PID_WITH_TURNS]

    def test_unknown_host_returns_empty(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)
        assert backend.get_podcasts_by_host("nobody here") == []

    def test_episode_search_returns_ids(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)
        hits = backend.search_by_host("ira glass")
        # One row per episode that predicts Ira Glass as host (all three).
        assert {h["episode_id"] for h in hits} == {
            EID_WITH_TURNS, "cc00dd11ee22ff33", EID_NO_TURNS}
        assert all(h["name_original"] == "Ira Glass" for h in hits)
        assert EID_WITH_TURNS in {h["episode_id"] for h in hits}

    def test_episode_search_respects_limit(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)
        assert backend.search_by_host("ira glass", limit=0) == []
        assert len(backend.search_by_host("ira glass", limit=1)) == 1

    def test_missing_index_raises_helpfully(self, tmp_parquet_layout):
        os.unlink(os.path.join(tmp_parquet_layout, "metadata",
                               "host_index.parquet"))
        backend = ParquetBackend(tmp_parquet_layout)
        with pytest.raises(IndexNotBuiltError):
            backend.get_podcasts_by_host("ira")


ALL_EPISODES = {EID_WITH_TURNS, "cc00dd11ee22ff33", EID_NO_TURNS}


class TestSearchEpisodesRoutesThroughIndex:
    """search_episodes(host_name=/guest_name=) is answered by the indexes."""

    def test_host_name_returns_the_predicted_host_episodes(
            self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)
        hits = backend.search_episodes(host_name="Ira")
        assert {h["episode_id"] for h in hits} == ALL_EPISODES

    def test_guest_name_returns_the_predicted_guest_episodes(
            self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)
        hits = backend.search_episodes(guest_name="Guest")
        assert {h["episode_id"] for h in hits} == ALL_EPISODES

    def test_host_name_uses_the_index_not_the_row_scan(self, tmp_parquet_layout):
        # The point of the index is that the per-episode list-column scan does
        # not run when the index is present.
        backend = ParquetBackend(tmp_parquet_layout)
        with patch.object(
                backend, "_filter_episodes_by_name_scan",
                wraps=backend._filter_episodes_by_name_scan) as scan:
            backend.search_episodes(host_name="Ira")
        scan.assert_not_called()

    def test_host_name_combines_with_other_criteria(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)
        hits = backend.search_episodes(host_name="Ira", podcast_id=PID_WITH_TURNS)
        assert {h["episode_id"] for h in hits} == {
            EID_WITH_TURNS, "cc00dd11ee22ff33"}

    def test_unknown_host_name_returns_nothing(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)
        assert backend.search_episodes(host_name="nobody") == []

    def test_falls_back_to_scan_when_index_absent(self, tmp_parquet_layout):
        # Older datasets ship no host_episode_index; the row scan still answers.
        os.unlink(os.path.join(tmp_parquet_layout, "metadata",
                               "host_episode_index.parquet"))
        backend = ParquetBackend(tmp_parquet_layout)
        with patch.object(
                backend, "_filter_episodes_by_name_scan",
                wraps=backend._filter_episodes_by_name_scan) as scan:
            hits = backend.search_episodes(host_name="Ira")
        scan.assert_called_once()
        assert {h["episode_id"] for h in hits} == ALL_EPISODES


class TestGuestIndex:
    """Diarized-guest lookups from guest_index / guest_episode_index."""

    def test_podcast_lookup(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)
        assert backend.get_podcasts_by_guest("jane") == [PID_WITH_TURNS]

    def test_podcast_lookup_case_insensitive(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)
        assert backend.get_podcasts_by_guest("JANE GUEST") == [PID_WITH_TURNS]

    def test_exact_rejects_substring(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)
        assert backend.get_podcasts_by_guest("jane", exact=True) == []
        assert backend.get_podcasts_by_guest("Jane Guest", exact=True) == [
            PID_WITH_TURNS]

    def test_episode_search_returns_ids(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)
        assert backend.search_by_guest("jane guest") == [{
            "episode_id": EID_WITH_TURNS,
            "podcast_id": PID_WITH_TURNS,
            "name_original": "Jane Guest",
        }]

    def test_predicted_guest_is_not_in_the_diarized_index(
            self, tmp_parquet_layout):
        # "A Guest" is predicted on every episode but never diarized, so the
        # appearance index does not list them -- the whole point of the split.
        backend = ParquetBackend(tmp_parquet_layout)
        assert backend.get_podcasts_by_guest("A Guest") == []

    def test_bulk_mapping(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)
        assert backend.diarized_guest_podcasts() == {
            "jane guest": {PID_WITH_TURNS}}

    def test_missing_index_raises_helpfully(self, tmp_parquet_layout):
        os.unlink(os.path.join(tmp_parquet_layout, "metadata",
                               "guest_index.parquet"))
        backend = ParquetBackend(tmp_parquet_layout)
        with pytest.raises(IndexNotBuiltError):
            backend.get_podcasts_by_guest("jane")


class TestTutorialReadsGuestIndex:
    """The tutorial subset builder uses the index instead of a part scan."""

    def _load_tutorial(self):
        import importlib
        import sys
        sys.path.insert(0, os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "scripts"))
        return importlib.import_module("build_tutorial_subset")

    def test_reads_index_without_scanning_parts(self, tmp_parquet_layout):
        tut = self._load_tutorial()
        backend = ParquetBackend(tmp_parquet_layout)
        # If it scanned parts it would call read_columns on the source; the
        # index path must not.
        with patch.object(backend._source, "read_columns",
                          side_effect=AssertionError("scanned parts")) as rc:
            labelled = tut.diarized_guest_index(backend, backend.shard_map)
        rc.assert_not_called()
        assert labelled == {"jane guest": {PID_WITH_TURNS}}

    def test_falls_back_to_part_scan_without_the_index(self, tmp_parquet_layout):
        tut = self._load_tutorial()
        os.unlink(os.path.join(tmp_parquet_layout, "metadata",
                               "guest_index.parquet"))
        backend = ParquetBackend(tmp_parquet_layout)
        # The fixture's episode part has guest_speaker_labels="{}", so a scan
        # finds no diarized guests -- but it must run rather than raise.
        labelled = tut.diarized_guest_index(backend, backend.shard_map)
        assert labelled == {}
