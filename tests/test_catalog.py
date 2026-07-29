"""
Reading the metadata catalogs.

Two things are being tested. That the catalogs are reachable at all -- they used
to require knowing where HuggingFace keeps its cache -- and that reading one
does not require constructing a dataset, since the guest index is a megabyte and
the corpus is 57 GB.
"""

import os
from unittest.mock import patch

import pytest

from conftest import PID_TURNS_2, PID_WITH_TURNS
from sporc import frames, schema
from sporc.exceptions import IndexNotBuiltError
from sporc.parquet_backend import ParquetBackend

# Every catalog the fixture layout actually writes.
PRESENT = ["podcasts", "episodes", "episode_metrics", "speaker_name_index",
           "host_index", "host_episode_index", "guest_index",
           "guest_episode_index", "category_index", "hostname_index",
           "shard_map"]


@pytest.fixture
def backend(tmp_parquet_layout):
    return ParquetBackend(tmp_parquet_layout)


class TestCatalogFrame:
    @pytest.mark.parametrize("name", PRESENT)
    def test_reads_each_catalog(self, backend, name):
        df = frames.catalog_frame(backend, name)
        assert not df.empty
        assert set(df.columns) <= set(schema.COLUMNS[schema.resolve_table(name)])

    def test_accepts_aliases(self, backend):
        by_alias = frames.catalog_frame(backend, "episode_catalog")
        by_name = frames.catalog_frame(backend, "episodes")
        assert len(by_alias) == len(by_name)

    def test_projection(self, backend):
        df = frames.catalog_frame(backend, "podcasts",
                                  columns=["podcast_id", "pod_title"])
        assert list(df.columns) == ["podcast_id", "pod_title"]

    def test_unknown_column_is_refused(self, backend):
        with pytest.raises(ValueError, match="pod_titel|pod_title"):
            frames.catalog_frame(backend, "podcasts", columns=["pod_titel"])

    def test_unknown_catalog_lists_the_options(self, backend):
        with pytest.raises(ValueError) as exc:
            frames.catalog_frame(backend, "not_a_catalog")
        assert "guest_index" in str(exc.value)

    def test_a_tree_is_not_a_catalog(self, backend):
        """Asking for turns here should point at the frame API, not fail oddly."""
        with pytest.raises(ValueError, match="turns_frame"):
            frames.catalog_frame(backend, "turns")

    def test_missing_catalog_names_the_file_and_the_fix(self, backend,
                                                        tmp_parquet_layout):
        os.unlink(os.path.join(tmp_parquet_layout, "metadata",
                               "guest_index.parquet"))
        b = ParquetBackend(tmp_parquet_layout)
        with pytest.raises(IndexNotBuiltError) as exc:
            frames.catalog_frame(b, "guest_index")
        assert "guest_index.parquet" in str(exc.value)

    def test_build_hint_survives_for_scripted_indexes(self, backend,
                                                      tmp_parquet_layout):
        """
        The speaker index is built by a script rather than shipped, so its
        error has to say which script. That distinction was carried by hand in
        eight separate loaders before; it now comes from the registry.
        """
        os.unlink(os.path.join(tmp_parquet_layout, "metadata",
                               "speaker_name_index.parquet"))
        b = ParquetBackend(tmp_parquet_layout)
        with pytest.raises(IndexNotBuiltError) as exc:
            frames.catalog_frame(b, "speaker_name_index")
        assert "build_indexes.py" in str(exc.value)
        assert "--phase 1" in str(exc.value)


class TestMetadataPath:
    def test_resolves_through_the_data_source(self, backend):
        """
        Not a raw os.path.join. Going through the source is what lets a lazily
        loaded dataset fetch a catalog it did not download up front.
        """
        with patch.object(backend._source, "path",
                          wraps=backend._source.path) as p:
            backend.metadata_path("guest_index")
        p.assert_called_once_with("metadata/guest_index.parquet")

    def test_required_false_returns_none(self, backend, tmp_parquet_layout):
        os.unlink(os.path.join(tmp_parquet_layout, "metadata",
                               "guest_index.parquet"))
        b = ParquetBackend(tmp_parquet_layout)
        assert b.metadata_path("guest_index", required=False) is None


class TestLoadCatalog:
    def test_from_a_local_directory_without_a_dataset(self, tmp_parquet_layout):
        """
        The headline case: no SPORCDataset, no network, no token.
        """
        df = frames.load_catalog("guest_index", parquet_dir=tmp_parquet_layout)
        assert list(df["name_normalized"]) == ["jane guest"]

    def test_projection(self, tmp_parquet_layout):
        df = frames.load_catalog("podcasts", columns=["podcast_id"],
                                 parquet_dir=tmp_parquet_layout)
        assert list(df.columns) == ["podcast_id"]

    def test_missing_file_locally(self, tmp_parquet_layout):
        os.unlink(os.path.join(tmp_parquet_layout, "metadata",
                               "guest_index.parquet"))
        with pytest.raises(IndexNotBuiltError):
            frames.load_catalog("guest_index", parquet_dir=tmp_parquet_layout)

    def test_hub_path_fetches_exactly_one_file(self, tmp_parquet_layout):
        """
        Reading a 1 MB index must not pull the ~195 MB metadata bundle, and must
        not reimplement the cache layout -- it goes through _download_one.
        """
        from sporc.dataset import SPORCDataset

        real = os.path.join(tmp_parquet_layout, "metadata",
                            "guest_index.parquet")
        with patch.object(SPORCDataset, "_download_one",
                          return_value=real) as dl:
            df = frames.load_catalog("guest_index")
        dl.assert_called_once()
        assert dl.call_args[0][0] == "metadata/guest_index.parquet"
        assert len(df) == 1

    def test_hub_path_reports_an_absent_catalog(self):
        from sporc.dataset import SPORCDataset

        with patch.object(SPORCDataset, "_download_one", return_value=None):
            with pytest.raises(IndexNotBuiltError):
                frames.load_catalog("guest_index")

    def test_importable_from_the_package_root(self):
        import sporc

        assert sporc.load_catalog is frames.load_catalog


class TestListing:
    def test_list_catalogs_excludes_trees(self):
        got = frames.list_catalogs()
        assert "guest_index" in got and "podcasts" in got
        assert "turns" not in got and "acoustics" not in got

    def test_describe_columns_needs_no_dataset(self):
        df = frames.describe_columns("acoustics")
        assert "f0_semitone_from_27_5hz_sma3nz_mean" in set(df["name"])
        assert df["description"].str.len().gt(0).all()
