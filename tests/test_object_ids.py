"""
Episode.episode_id / Podcast.podcast_id.

Neither object used to carry an id: you passed an episode_id into
build_episode_object() and got back something with no way to recover it, so
callers keyed on title. Titles collide -- 11 of the 2,428 episodes in the
tutorial subset share a (podcast_title, title), one podcast having seven
episodes all called "Daily Encouragement" -- and a groupby on title silently
merges them.

The ids come from the catalog when the backend builds the object. The
derivation is the fallback, and it is exact: md5(rss_url)[:12] reproduces the
podcast_id of all 228,099 podcasts, md5(mp3_url)[:16] the episode_id of all
1,124,058 episodes, with the urls themselves unique in both cases.
"""

import hashlib

import pytest

from sporc.episode import Episode
from sporc.parquet_backend import ParquetBackend
from sporc.podcast import Podcast

from conftest import EID_WITH_TURNS, PID_WITH_TURNS


def _episode(**kw):
    base = dict(
        title="T", description="d", mp3_url="http://example.com/a.mp3",
        duration_seconds=1.0, transcript="x", podcast_title="P",
        podcast_description="pd", rss_url="http://example.com/feed.xml",
    )
    base.update(kw)
    return Episode(**base)


class TestDerivedIds:
    """A hand-built object has no catalog row, so it derives from the url."""

    def test_episode_id_is_md5_of_mp3_url(self):
        url = "http://example.com/ep1.mp3"
        expected = hashlib.md5(url.encode()).hexdigest()[:16]

        assert _episode(mp3_url=url).episode_id == expected

    def test_podcast_id_is_md5_of_rss_url(self):
        url = "http://example.com/feed.xml"
        expected = hashlib.md5(url.encode()).hexdigest()[:12]

        pod = Podcast(title="T", description="d", rss_url=url)

        assert pod.podcast_id == expected

    def test_episode_exposes_its_podcasts_id(self):
        # An episode carries its podcast's rss_url, so it can name the show it
        # belongs to without a trip back to the catalog.
        url = "http://example.com/feed.xml"
        expected = hashlib.md5(url.encode()).hexdigest()[:12]

        assert _episode(rss_url=url).podcast_id == expected

    def test_episode_and_podcast_agree_on_podcast_id(self):
        url = "http://example.com/feed.xml"

        pod = Podcast(title="T", description="d", rss_url=url)
        ep = _episode(rss_url=url)

        assert ep.podcast_id == pod.podcast_id

    def test_episode_id_length_matches_corpus_scheme(self):
        assert len(_episode().episode_id) == 16

    def test_podcast_id_length_matches_corpus_scheme(self):
        pod = Podcast(title="T", description="d", rss_url="http://e.com/f.xml")

        assert len(pod.podcast_id) == 12

    def test_different_mp3_urls_give_different_ids(self):
        a = _episode(mp3_url="http://example.com/1.mp3", title="Same Title")
        b = _episode(mp3_url="http://example.com/2.mp3", title="Same Title")

        # The whole point: same title, different episode.
        assert a.title == b.title
        assert a.episode_id != b.episode_id

    def test_same_title_episodes_are_distinct_dict_keys(self):
        # "Daily Encouragement" x7, in miniature.
        eps = [_episode(mp3_url=f"http://example.com/{i}.mp3",
                        title="Daily Encouragement") for i in range(7)]

        assert len({e.episode_id for e in eps}) == 7
        assert len({e.title for e in eps}) == 1


class TestCatalogIdsWin:
    """When the backend knows the real id, it is used rather than re-derived."""

    def test_explicit_id_overrides_derivation(self):
        ep = _episode(_episode_id="deadbeefdeadbeef")

        assert ep.episode_id == "deadbeefdeadbeef"

    def test_explicit_podcast_id_overrides_derivation(self):
        pod = Podcast(title="T", description="d", rss_url="http://e.com/f.xml",
                      _podcast_id="abc123abc123")

        assert pod.podcast_id == "abc123abc123"

    def test_blank_id_falls_back_to_derivation(self):
        # The backend passes None when the catalog column is empty.
        url = "http://example.com/x.mp3"

        assert _episode(mp3_url=url, _episode_id=None).episode_id == \
            hashlib.md5(url.encode()).hexdigest()[:16]


@pytest.mark.integration
class TestIdsAgainstRealLayout:
    def test_built_episode_reports_the_id_it_was_asked_for(
        self, tmp_parquet_layout
    ):
        # build_episode_object(pid, eid) used to return an object that had lost
        # the eid it was looked up by.
        backend = ParquetBackend(tmp_parquet_layout)

        ep = backend.build_episode_object(PID_WITH_TURNS, EID_WITH_TURNS)

        assert ep.episode_id == EID_WITH_TURNS

    def test_built_podcast_reports_its_catalog_id(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)

        pod = backend.build_podcast_object(PID_WITH_TURNS)

        assert pod.podcast_id == PID_WITH_TURNS

    def test_built_episode_knows_its_podcast_id(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)

        ep = backend.build_episode_object(PID_WITH_TURNS, EID_WITH_TURNS)

        assert ep.podcast_id == PID_WITH_TURNS

    def test_podcasts_episodes_all_carry_its_id(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)

        pod = backend.build_podcast_object(PID_WITH_TURNS)

        assert {e.podcast_id for e in pod.episodes} == {pod.podcast_id}

    def test_episodes_of_a_podcast_have_distinct_ids(self, tmp_parquet_layout):
        backend = ParquetBackend(tmp_parquet_layout)

        pod = backend.build_podcast_object(PID_WITH_TURNS)
        ids = [e.episode_id for e in pod.episodes]

        assert len(ids) == len(set(ids))
        assert all(ids)
