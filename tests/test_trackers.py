"""Unit tests for watcher_failure.trackers.RedmineConnector.

No network, no real Redmine package required — conftest.py stubs
`redminelib` before this module (or trackers.py) ever imports it.
"""
import json
import logging
from pathlib import Path

import pytest

from watcher_failure import trackers
from watcher_failure.trackers import RedmineConnector


def _write_config(tmp_path: Path, *, password: str = "testpass") -> Path:
    config_path = tmp_path / ".redmine"
    config_path.write_text(
        "[redmine]\n"
        "url = https://tracker.example.com\n"
        "username = testuser\n"
        f"password = {password}\n"
        "project_name = TestProj\n"
    )
    return config_path


def make_connector(tmp_path: Path, cache_file=None) -> RedmineConnector:
    config_path = _write_config(tmp_path)
    cache_file = cache_file or (tmp_path / "tracker_cache.json")
    return RedmineConnector(config_path=config_path, cache_file=cache_file)


class FakeStatus:
    def __init__(self, name):
        self.name = name


class FakeIssue:
    def __init__(self, id_, title, description="", status=None):
        self.id = id_
        self.title = title
        self.description = description
        if status is not None:
            self.status = FakeStatus(status)


# ---------------------------------------------------------------------
# security regression: don't leak ~/.redmine contents to logs
# ---------------------------------------------------------------------
def test_load_config_does_not_leak_credentials_to_logs(tmp_path, caplog):
    secret = "s3cr3t-do-not-log-me"
    config_path = _write_config(tmp_path, password=secret)

    with caplog.at_level(logging.DEBUG):
        RedmineConnector._load_config(config_path)

    assert secret not in caplog.text


# ---------------------------------------------------------------------
# correctness regression: search must be scoped to the Redmine project
# ---------------------------------------------------------------------
def test_fetch_issues_scopes_to_project_id(tmp_path):
    conn = make_connector(tmp_path)
    assert conn.project_id is not None

    captured = {}

    def fake_search(**kwargs):
        captured.update(kwargs)
        return []

    conn.redmine.issue.search = fake_search
    conn._fetch_issues("some query", status=None, limit=10)

    assert captured.get("project_id") == conn.project_id


# ---------------------------------------------------------------------
# normalization: the noise sources that used to fork one failure into
# multiple cache entries must now collapse to the same string
# ---------------------------------------------------------------------
def test_normalize_strips_timestamp_and_daemon_id(tmp_path):
    conn = make_connector(tmp_path)
    raw = "2025-05-18T23:33:40.185645+0000 osd.6 (osd.6) cluster [WRN] OSD bench slow"
    norm = conn._normalize_for_search(raw)
    assert "2025-05-18" not in norm
    assert "osd.6" not in norm


def test_normalize_strips_smithi_hostname(tmp_path):
    conn = make_connector(tmp_path)
    a = conn._normalize_for_search("Command failed on smithi000 with status 1: 'foo'")
    b = conn._normalize_for_search("Command failed on smithi123 with status 1: 'foo'")
    assert a == b
    assert "smithi" not in a


def test_normalize_collapses_mkdir_prefix_and_cluster_arg_quoting(tmp_path):
    conn = make_connector(tmp_path)
    with_mkdir = (
        "Command failed (workunit test mon/mkfs.sh) on smithi000 with status 1: "
        "'mkdir -p -- /home/ubuntu/cephtest/mnt.0/client.0/tmp && "
        "cd -- /home/ubuntu/cephtest/mnt.0/client.0/tmp && "
        "CEPH_ARGS=\"--cluster ceph\" timeout 3h qa/standalone/mon/mkfs.sh'"
    )
    without_mkdir = (
        "Command failed (workunit test mon/mkfs.sh) on smithi007 with status 1: "
        "'cd -- /home/ubuntu/cephtest/mnt.0/client.0/tmp && "
        "CEPH_ARGS=\"--cluster=ceph\" timeout 3h qa/standalone/mon/mkfs.sh'"
    )
    assert conn._normalize_for_search(with_mkdir) == conn._normalize_for_search(without_mkdir)


# ---------------------------------------------------------------------
# degenerate-reason guard: known placeholders must never reach Redmine
# ---------------------------------------------------------------------
@pytest.mark.parametrize("reason", ["None", "none", "BACKTRACE", "backtrace", ""])
def test_degenerate_reasons_are_rejected_without_calling_redmine(tmp_path, reason):
    conn = make_connector(tmp_path)

    def fail_if_called(*args, **kwargs):
        raise AssertionError("Redmine should not be queried for a degenerate reason")

    conn._fetch_issues = fail_if_called
    assert conn.search_and_refine(reason) == {}


# ---------------------------------------------------------------------
# confidence threshold: a weak fuzzy match must be rejected, not
# silently returned as the "best available" guess
# ---------------------------------------------------------------------
def test_find_best_match_rejects_low_confidence_score(tmp_path):
    conn = make_connector(tmp_path)
    issues = [FakeIssue(1, "zzz qqq xkcd 918273 wobble jungle", "yy nn ppp 00")]
    assert conn._find_best_match("some specific ceph failure text", issues) is None


def test_find_best_match_accepts_strong_match(tmp_path):
    conn = make_connector(tmp_path)
    query = "OSD bench result is not within the threshold limit range"
    issues = [FakeIssue(42, "OSD bench result is not within the threshold limit range", "")]
    best = conn._find_best_match(query, issues)
    assert best is not None
    issue_id, score, status_name, is_closed = best
    assert issue_id == 42
    assert score >= conn.MIN_MATCH_SCORE
    assert is_closed is False


# ---------------------------------------------------------------------
# closed/resolved issues: don't hand one back as the active match, but
# don't silently drop it either
# ---------------------------------------------------------------------
def test_find_best_match_prefers_open_issue_over_higher_scoring_closed_one(tmp_path):
    conn = make_connector(tmp_path)
    query = "OSD bench result is not within the threshold limit range"
    issues = [
        FakeIssue(1, "OSD bench result is not within the threshold limit range", status="Resolved"),
        FakeIssue(2, "OSD bench result is not within the threshold limit", status="New"),
    ]
    issue_id, score, status_name, is_closed = conn._find_best_match(query, issues)
    assert issue_id == 2
    assert is_closed is False


def test_find_best_match_surfaces_closed_issue_when_nothing_open_qualifies(tmp_path):
    conn = make_connector(tmp_path)
    query = "OSD bench result is not within the threshold limit range"
    issues = [FakeIssue(1, "OSD bench result is not within the threshold limit range", status="Resolved")]
    issue_id, score, status_name, is_closed = conn._find_best_match(query, issues)
    assert issue_id == 1
    assert is_closed is True
    assert status_name == "Resolved"


def test_search_and_refine_records_closed_match_instead_of_using_it(tmp_path):
    conn = make_connector(tmp_path)
    query = "OSD bench result is not within the threshold limit range"
    issues = [FakeIssue(1, query, status="Closed")]
    conn._fetch_issues = lambda *a, **k: issues

    result = conn.search_and_refine(query)

    assert "issue_id" not in result
    assert result["closed_match"] == {
        "issue_id": 1,
        "status": "Closed",
        "link": "https://tracker.example.com/issues/1",
    }

    # and it's what got persisted, not an accepted open match
    key = conn._normalize_for_search(query)
    assert conn.cache[key] == {"issue_id": None, "closed_match": {"issue_id": 1, "status": "Closed"}}


# ---------------------------------------------------------------------
# link is derived, never stored — must rebuild from current config
# ---------------------------------------------------------------------
def test_to_result_rebuilds_link_from_issue_id(tmp_path):
    conn = make_connector(tmp_path)
    assert conn._to_result({"issue_id": 123}) == {
        "issue_id": 123,
        "link": "https://tracker.example.com/issues/123",
    }
    assert conn._to_result({}) == {}


def test_cache_hit_returns_link_without_calling_redmine(tmp_path):
    conn = make_connector(tmp_path)
    raw = "some already-known failure text"
    key = conn._normalize_for_search(raw)
    conn.cache[key] = {"issue_id": 555}

    def fail_if_called(*args, **kwargs):
        raise AssertionError("cache hit must not hit Redmine")

    conn._fetch_issues = fail_if_called
    result = conn.search_and_refine(raw)
    assert result == {"issue_id": 555, "link": "https://tracker.example.com/issues/555"}


# ---------------------------------------------------------------------
# cache persistence: atomic save, only issue_id on disk (no link),
# and no leftover .tmp file
# ---------------------------------------------------------------------
def test_save_cache_is_atomic_and_reloadable(tmp_path):
    cache_file = tmp_path / "tracker_cache.json"
    conn = make_connector(tmp_path, cache_file=cache_file)
    conn.cache["some normalized key"] = {"issue_id": 7}
    conn._save_cache()

    assert cache_file.exists()
    assert not cache_file.with_suffix(cache_file.suffix + ".tmp").exists()

    on_disk = json.loads(cache_file.read_text())
    assert on_disk == {"some normalized key": {"issue_id": 7}}
    assert "link" not in on_disk["some normalized key"]

    reloaded = make_connector(tmp_path, cache_file=cache_file)
    assert reloaded.cache == {"some normalized key": {"issue_id": 7}}


# ---------------------------------------------------------------------
# path resolution: a relative cache_file must resolve next to
# trackers.py, not the process cwd — prevents a second, diverging
# cache file from appearing depending on where the script is run from
# ---------------------------------------------------------------------
def test_relative_cache_path_resolves_next_to_module(tmp_path):
    config_path = _write_config(tmp_path)
    conn = RedmineConnector(config_path=config_path, cache_file="a_relative_name.json")
    expected = Path(trackers.__file__).resolve().parent / "a_relative_name.json"
    assert conn.cache_path == expected


# ---------------------------------------------------------------------
# search query sent to Redmine strips shared boilerplate: "Command
# failed on with status N: '...'" appears in effectively every
# teuthology command-failure ticket, and with all_words=True (AND
# search) those common words dilute relevance enough that the actual
# right ticket can get pushed past `limit` by tickets that only share
# the boilerplate. The cache key must stay the full, specific string.
# ---------------------------------------------------------------------
def test_fetch_issues_receives_boilerplate_stripped_query(tmp_path):
    conn = make_connector(tmp_path)
    raw = "Command failed on smithi000 with status 234: 'sudo nvme connect -t loop -n lv_1'"

    captured = {}

    def fake_fetch(query, **kwargs):
        captured["query"] = query
        return []

    conn._fetch_issues = fake_fetch
    conn.search_and_refine(raw)

    assert captured["query"] == "sudo nvme connect -t loop -n lv_1"


def test_fetch_issues_falls_back_to_full_query_when_all_boilerplate(tmp_path):
    conn = make_connector(tmp_path)
    raw = "Command failed on smithi000 with status 234:"

    captured = {}

    def fake_fetch(query, **kwargs):
        captured["query"] = query
        return []

    conn._fetch_issues = fake_fetch
    conn.search_and_refine(raw)

    # nothing left after stripping boilerplate -> must not send an empty query
    assert captured["query"]


# ---------------------------------------------------------------------
# description scoring: a long, pasted-log description that verbatim
# contains the query must not score near zero just because the
# description is much longer than the query.
# ---------------------------------------------------------------------
def test_containment_ratio_finds_verbatim_match_in_long_description():
    needle = "sudo nvme connect -t loop -n lv_1"
    haystack = "noise " * 100 + needle + " more noise " * 100  # >200 chars, past autojunk threshold
    assert len(haystack) > 200
    assert RedmineConnector._containment_ratio(needle, haystack) == 1.0


def test_containment_ratio_ignores_short_needles():
    # a short needle can trivially appear by chance in unrelated text
    assert RedmineConnector._containment_ratio("sudo ls", "noise " * 100 + "sudo ls" + "noise " * 100) == 0.0


def test_find_best_match_uses_description_containment_not_whole_string_ratio(tmp_path):
    conn = make_connector(tmp_path)
    query = "sudo nvme connect -t loop -n lv_1"
    long_description = (
        "/a/some-job-dir/8337258\n\nError in scrape.log:\n<pre><code>\n"
        "Failure: Command failed on smithi079 with status 234: 'sudo mkdir -p /x && "
        "sudo nvme connect -t loop -n lv_1 -q hostnqn'\n</code></pre>\n"
        + "irrelevant log noise " * 30
    )
    issues = [FakeIssue(71816, "Failed to write to /dev/nvme-fabrics: Invalid argument", long_description)]
    match = conn._find_best_match(query, issues)
    assert match is not None
    issue_id, score, status_name, is_closed = match
    assert issue_id == 71816
