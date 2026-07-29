"""
Tests for the static column registry.

The important test here is the drift guard: the registry describes tables it
cannot see, so nothing stops it going stale except comparing it against a real
layout. A wrong description is worse than none -- someone reads it instead of
the data.
"""

import glob
import os

import pyarrow.parquet as pq
import pytest

from sporc import schema


class TestTableResolution:
    def test_canonical_names_resolve_to_themselves(self):
        for name in schema.list_tables():
            assert schema.resolve_table(name) == name

    @pytest.mark.parametrize("alias,expected", [
        ("turns_text", "turns"),
        ("text", "turns"),
        ("episode_catalog", "episodes"),
        ("podcast_catalog", "podcasts"),
        ("audio", "acoustics"),
        ("TURNS", "turns"),
        ("  turns  ", "turns"),
    ])
    def test_aliases(self, alias, expected):
        assert schema.resolve_table(alias) == expected

    def test_unknown_table_suggests_a_close_match(self):
        with pytest.raises(ValueError) as exc:
            schema.resolve_table("turn_text")
        assert "turns_text" in str(exc.value) or "turns" in str(exc.value)

    def test_unknown_table_lists_the_options(self):
        with pytest.raises(ValueError) as exc:
            schema.resolve_table("zzzz")
        assert "acoustics" in str(exc.value)

    def test_every_table_has_columns_and_every_column_a_description(self):
        for name in schema.list_tables():
            cols = schema.COLUMNS[name]
            assert cols, f"{name} has no columns"
            for col, spec in cols.items():
                assert spec.description.strip(), f"{name}.{col} has no description"
                assert spec.nbytes > 0, f"{name}.{col} has no size"

    def test_table_keys_are_real_columns(self):
        for name, spec in schema.TABLES.items():
            known = schema.COLUMNS[name]
            for key in spec.keys + spec.sort_keys:
                assert key in known, f"{name}: key {key!r} is not a column"


class TestColumnValidation:
    def test_none_passes_through(self):
        assert schema.validate_columns("turns", None) is None

    def test_valid_columns_come_back_as_a_list(self):
        got = schema.validate_columns("turns", ("episode_id", "start_time"))
        assert got == ["episode_id", "start_time"]

    def test_unknown_column_names_itself_and_a_suggestion(self):
        with pytest.raises(ValueError) as exc:
            schema.validate_columns("turns", ["turn_txt"])
        msg = str(exc.value)
        assert "turn_txt" in msg
        assert "turn_text" in msg

    def test_empty_list_is_refused_rather_than_meaning_everything(self):
        """columns=[] almost always means a filter built up to nothing."""
        with pytest.raises(ValueError):
            schema.validate_columns("turns", [])


class TestEstimateBytes:
    def test_text_dominates(self):
        """
        The whole reason the guard is byte-based: dropping one column changes
        the answer by an order of magnitude, which a row count cannot see.
        """
        rows = 1_000_000
        narrow = schema.estimate_bytes(
            "turns", ["episode_id", "turn_count", "start_time"], rows)
        wide = schema.estimate_bytes("turns", None, rows)
        assert wide > narrow * 5

    def test_unknown_columns_get_a_pessimistic_default(self):
        n = schema.estimate_bytes("turns", ["not_a_real_column"], 1000)
        assert n == schema.DEFAULT_COLUMN_BYTES * 1000

    def test_zero_rows(self):
        assert schema.estimate_bytes("turns", None, 0) == 0

    @pytest.mark.parametrize("n,expect", [
        (512, "512 B"), (2048, "2 KB"), (5 * 1024 ** 3, "5.0 GB"),
    ])
    def test_format_bytes(self, n, expect):
        assert schema.format_bytes(n) == expect


# ---------------------------------------------------------------------------
# Drift guard
# ---------------------------------------------------------------------------

def _arrow_names(path):
    return list(pq.ParquetFile(path).schema_arrow.names)


class TestRegistryMatchesRealFiles:
    """
    Compare the registry against actual Parquet schemas.

    Runs against the fixture layout always, and additionally against
    subsets/tutorial/ when a developer has it -- the fixture is a faithful but
    partial copy, so the real subset catches columns the fixture omits.
    """

    def test_matches_fixture_layout(self, tmp_parquet_layout):
        checks = [
            ("turns", "turns/text"),
            ("turns_metrics", "turns/metrics"),
            ("acoustics", "acoustics"),
            ("episodes_full", "episodes"),
        ]
        for table, subdir in checks:
            parts = glob.glob(os.path.join(tmp_parquet_layout, subdir, "*.parquet"))
            assert parts, f"fixture has no {subdir} part"
            known = set(schema.COLUMNS[table])
            actual = set(_arrow_names(parts[0]))
            missing = actual - known
            assert not missing, (
                f"{table}: file has columns the registry does not describe: "
                f"{sorted(missing)}")

        for table, stem in [("episodes", "episode_catalog"),
                            ("podcasts", "podcast_catalog"),
                            ("speaker_name_index", "speaker_name_index"),
                            ("host_index", "host_index"),
                            ("guest_index", "guest_index"),
                            ("host_episode_index", "host_episode_index"),
                            ("guest_episode_index", "guest_episode_index"),
                            ("category_index", "category_index"),
                            ("hostname_index", "hostname_index"),
                            ("shard_map", "shard_map")]:
            path = os.path.join(tmp_parquet_layout, "metadata", f"{stem}.parquet")
            if not os.path.exists(path):
                continue
            missing = set(_arrow_names(path)) - set(schema.COLUMNS[table])
            assert not missing, (
                f"{table}: file has columns the registry does not describe: "
                f"{sorted(missing)}")

    def test_matches_published_subset_if_present(self):
        root = os.path.join(os.path.dirname(__file__), "..", "subsets", "tutorial")
        if not os.path.isdir(root):
            pytest.skip("subsets/tutorial not built")
        checks = [
            ("turns", "turns/text"), ("turns_metrics", "turns/metrics"),
            ("acoustics", "acoustics"), ("episodes_full", "episodes"),
        ]
        for table, subdir in checks:
            parts = glob.glob(os.path.join(root, subdir, "*.parquet"))
            if not parts:
                continue
            actual = _arrow_names(parts[0])
            known = schema.COLUMNS[table]
            assert not set(actual) - set(known), (
                f"{table}: undescribed columns "
                f"{sorted(set(actual) - set(known))}")
            # The registry should not describe columns that do not exist
            # either; that is how a rename gets missed.
            assert not set(known) - set(actual), (
                f"{table}: registry describes columns not in the file "
                f"{sorted(set(known) - set(actual))}")
