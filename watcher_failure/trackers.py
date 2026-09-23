#!/usr/bin/env python3
"""
trackers.py – helper for mapping teuthology/ceph test failure strings ⇢ Ceph Redmine

Key features
------------
* Read creds from ``~/.redmine``
* Aggressively **normalise** noisy failure strings (timestamps, daemon ids, long
  numbers, log‑level tags …) so the search query is generic but still
  meaningful.
* Hit the Redmine `/search.json` API once, then pick the **closest** match based
  on ``difflib.SequenceMatcher``.
* Persist the failure‑string → known‑issue mapping to a JSON store (``tracker_cache.json``)
  so we don't hammer Redmine when repeatedly processing the same logs. Despite the
  "cache" name this is a durable known‑issue map, not a TTL cache — entries (including
  "no match found") are meant to stick around, so don't add expiry without checking
  with the reason a miss was cached in the first place.
"""
from __future__ import annotations

import configparser
import json
import logging
import re
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from redminelib import Redmine

logger = logging.getLogger(__name__)


class RedmineConnector:
    """Convenience wrapper around *python‑redmine* with caching and fuzzy search."""

    # below this SequenceMatcher score, a "best" match is still too weak to
    # trust — reject it rather than silently link to an unrelated issue.
    MIN_MATCH_SCORE = 0.35

    # upstream failure-scanner placeholders that carry no real information
    # (e.g. a truncated backtrace, a null reason field) — searching Redmine
    # for these returns a plausible-looking but meaningless "best" match, so
    # skip the lookup entirely instead of caching a bogus link under a
    # generic key that future unrelated failures could collide with.
    DEGENERATE_REASONS = {"none", "backtrace", "unknown", ""}

    # Redmine statuses that mean "not an active bug right now" — a match
    # landing on one of these is suspect (could be a stale/unrelated old
    # ticket) so it's rejected as the accepted match, but recorded rather
    # than silently discarded, since a resolved bug recurring is itself
    # useful information.
    CLOSED_STATUS_NAMES = {
        "resolved", "closed", "rejected", "duplicate",
        "won't fix", "won't fix - eol", "can't reproduce",
    }

    # generic wrappers that many unrelated tracker issues share verbatim
    # (Redmine "umbrella:" tickets, and tickets titled with a raw pasted
    # cluster-log line, especially) — e.g. "Command failed on smithi000
    # with status 234: '<actual command>'" or "cluster [WRN] Health check
    # failed: <msg> (<CODE>)" carry no signal about *which* command/health
    # check this is, but SequenceMatcher scores whole-string similarity and
    # Redmine's all_words search ANDs every word, so this shared filler
    # alone can push wrong matches through, or crowd out the real match
    # entirely. Stripped for scoring AND for the Redmine search query
    # (search_and_refine), never from the cache key — the key stays the
    # full specific string so different failures don't collapse together.
    BOILERPLATE_RE = re.compile(
        r"(?:^command failed on\s*with status \d+:?\s*"
        r"|^timeout \d+ running\s*"
        r"|^cluster (?:err|wrn|inf)\s+health check failed:?\s*"
        r"|\s+in cluster log$)",
        re.IGNORECASE,
    )

    # low-signal words to ignore for token-overlap scoring (see
    # _token_overlap_ratio). Not meant to be exhaustive — just enough that
    # matching on shared *distinctive* words (nvme, mkfs, ceph-authtool)
    # isn't drowned out by words nearly every failure/ticket contains.
    STOPWORDS = frozenset({
        "a", "an", "the", "on", "in", "with", "for", "and", "or", "to", "of",
        "is", "at", "by", "command", "failed", "failure", "test", "timeout",
        "status", "running", "cluster", "log", "error", "smithi",
    })

    _TOKEN_RE = re.compile(r"[a-z0-9_./-]+")

    # ---------------------------------------------------------------------
    # life‑cycle helpers                                                   |
    # ---------------------------------------------------------------------
    def __init__(
        self,
        config_path: str | Path = "~/.redmine",
        cache_file: str | Path = "tracker_cache.json",
    ) -> None:
        logger.debug("Loading Redmine config from: %s", config_path)
        self.config = self._load_config(config_path)
        cache_path = Path(cache_file).expanduser()
        if not cache_path.is_absolute():
            # match Config's resolution so a standalone run (no explicit
            # cache_file, e.g. `python trackers.py`) can't create a second,
            # diverging store next to the real one.
            cache_path = Path(__file__).resolve().parent / cache_path
        self.cache_path = cache_path
        logger.debug("Using cache file: %s", self.cache_path)
        self.cache: Dict[str, Any] = self._load_cache()

        red_cfg = self.config["redmine"] if self.config.has_section("redmine") else {}
        logger.debug("   Redmine config: %s", red_cfg)

        self.redmine = Redmine(
            red_cfg.get("url", "https://tracker.ceph.com"),
            username=red_cfg.get("username", ""),
            key=red_cfg.get("password", ""),
        )

        project_name = red_cfg.get("project_name", "Ceph")
        try:
            self.project_id = self.redmine.project.get(project_name).id  # type: ignore[attr-defined]
            logger.debug("Connected to Redmine project ID: %s", self.project_id)
        except Exception as exc:  # pragma: no cover – network issue
            logger.warning("Could not fetch Redmine project '%s': %s", project_name, exc)
            self.project_id = None

    # ---------------------------------------------------------------------
    # public entry‑point                                                   |
    # ---------------------------------------------------------------------
    def search_and_refine(
        self,
        search_string: str,
        *,
        status: Optional[str] = None,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """Return one of:

        * ``{"issue_id": int, "link": str}`` — an open issue matched.
        * ``{"closed_match": {"issue_id": int, "status": str, "link": str}}``
          — every candidate that scored well enough was closed/resolved;
          not treated as the active known issue, but surfaced so it isn't
          silently dropped.
        * ``{}`` — nothing matched at all.

        Only ``issue_id`` (or ``closed_match``) is persisted to the cache
        file — ``link`` is derived from it plus the current Redmine URL on
        every return, so a cached entry can't go stale if the URL changes.
        """
        if not isinstance(search_string, str):
            raise TypeError("search_string must be str")

        # 1. normalise ------------------------------------------------------
        # done first: the cache is keyed on the normalized string so two
        # raw failures that differ only in incidental noise (hostname,
        # mkdir/cd prefix, arg quoting, ...) map to the same known-issue
        # entry instead of each getting their own.
        q_norm = self._normalize_for_search(search_string)

        # 0. cheap cache look‑up ------------------------------------------------
        if q_norm in self.cache:
            logger.debug("Cache hit for '%s'", q_norm)
            return self._to_result(self.cache[q_norm])

        # reject known-degenerate reasons before ever hitting Redmine — no
        # information to search on, so any "best match" would be noise.
        if q_norm.lower() in self.DEGENERATE_REASONS:
            logger.debug("Refusing to search degenerate reason '%s'", q_norm)
            return {}

        # 2. fetch possible issues ---------------------------------------------
        # search on the boilerplate-stripped text, not the full q_norm: the
        # cache key stays specific (full command), but "Command failed on
        # with status N: '...'" appears in effectively every teuthology
        # command-failure ticket, and with all_words=True (AND search)
        # those common words dilute relevance enough that older or
        # differently-recent matches get pushed past `limit` by a flood of
        # tickets that only share the boilerplate, not the actual failure.
        search_query = self._strip_boilerplate(q_norm) or q_norm
        issues = self._fetch_issues(search_query, status=status, limit=limit)
        if not issues:
            logger.debug("No issues found for '%s'", search_string)
            self.cache[q_norm] = {}
            self._save_cache()
            return {}

        # 3. select the best match, preferring an open issue ---------------
        match = self._find_best_match(q_norm, issues)
        if match is None:
            logger.debug("Could not identify a close enough Redmine issue for '%s'", search_string)
            self.cache[q_norm] = {}
            self._save_cache()
            return {}

        issue_id, score, status_name, is_closed = match
        if is_closed:
            # every candidate that cleared MIN_MATCH_SCORE was closed —
            # don't hand back a stale/resolved ticket as if it were the
            # active known issue, but keep the pointer so it's visible.
            logger.warning(
                "Best match for '%s' is issue %s but it's '%s' (closed) — "
                "no open candidate cleared the threshold, recording as closed_match",
                search_string, issue_id, status_name,
            )
            entry = {"issue_id": None, "closed_match": {"issue_id": issue_id, "status": status_name}}
        else:
            logger.debug("Selected issue %s (score %.02f, status %s)", issue_id, score, status_name)
            entry = {"issue_id": issue_id}

        self.cache[q_norm] = entry
        self._save_cache()
        return self._to_result(entry)

    # ------------------------------------------------------------------
    # internal helpers                                                   |
    # ------------------------------------------------------------------
    @staticmethod
    def _load_config(path: str | Path) -> configparser.ConfigParser:
        cfg = configparser.ConfigParser()
        cfg.read(Path(path).expanduser())
        return cfg

    # cache -------------------------------------------------------------
    def _load_cache(self) -> Dict[str, Any]:
        try:
            with open(self.cache_path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _save_cache(self) -> None:
        # write-then-rename: a crash/kill mid-write leaves the old file
        # intact instead of a truncated, unparseable one.
        tmp_path = self.cache_path.with_suffix(self.cache_path.suffix + ".tmp")
        try:
            with open(tmp_path, "w", encoding="utf-8") as fh:
                json.dump(self.cache, fh, indent=2)
            tmp_path.replace(self.cache_path)
            logger.debug("Cache saved → %s", self.cache_path)
        except Exception as exc:  # pragma: no cover – filesystem perms
            logger.warning("Could not persist cache: %s", exc)

    def _to_result(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Expand a stored cache entry into the public result shape,
        rebuilding any ``link`` from the *current* Redmine URL."""
        issue_id = entry.get("issue_id")
        if issue_id is not None:
            return {
                "issue_id": issue_id,
                "link": f"{self.config['redmine']['url']}/issues/{issue_id}",
            }
        closed = entry.get("closed_match")
        if closed:
            return {
                "closed_match": {
                    "issue_id": closed["issue_id"],
                    "status": closed.get("status"),
                    "link": f"{self.config['redmine']['url']}/issues/{closed['issue_id']}",
                }
            }
        return {}

    # Redmine search ----------------------------------------------------
    def _fetch_issues(self, query: str, *, status: Optional[str], limit: int) -> List[Any]:
        params: Dict[str, Any] = {
            "limit": limit,
            "all_words": True,
            "wiki_pages": False,
            "attachments": False,
            "open_issues": False,
            "query": query,
        }
        if status:
            params["status_id"] = status
        if self.project_id is not None:
            params["project_id"] = self.project_id
        logger.debug("Searching Redmine with params: %s", params)
        return self.redmine.issue.search(**params)  # type: ignore[arg-type]

    # similarity scoring ------------------------------------------------
    def _find_best_match(
        self, original: str, issues: List[Any]
    ) -> Optional[Tuple[int, float, Optional[str], bool]]:
        """Return ``(issue_id, score, status_name, is_closed)`` for the
        best candidate that clears ``MIN_MATCH_SCORE``, or ``None`` if
        nothing does.

        Prefers the highest-scoring *open* issue. If every candidate that
        clears the threshold is closed/resolved, the best of those is
        still returned (with ``is_closed=True``) so the caller can record
        it rather than the lookup just going silent.
        """
        original_for_scoring = self._strip_boilerplate(original)
        query_tokens = self._tokenize(original_for_scoring)
        scored: List[Tuple[int, float, Optional[str], bool]] = []
        for it in issues:
            title = self._strip_boilerplate(self._trim_after_colon(it.title))
            desc = self._strip_boilerplate(it.description or "")
            score = max(
                SequenceMatcher(None, original_for_scoring, title).ratio(),
                # descriptions are often long pasted logs, not a short
                # human summary — whole-string ratio() penalizes by
                # combined length, so a verbatim match of our (short)
                # query buried in a (long) log dump still scores near
                # zero. Score by how much of the query it contains
                # instead, so an exact substring match scores ~1.0
                # regardless of how much surrounding log noise there is.
                self._containment_ratio(original_for_scoring, desc),
                # catches matches SequenceMatcher misses because wording
                # differs (reordered, paraphrased) but the same distinctive
                # words show up in the title regardless of phrasing. Title
                # only, not description: descriptions are often huge pasted
                # logs where nearly any handful of common ceph/test words
                # will coincidentally all appear *somewhere*, without that
                # meaning anything — confirmed this gives false 1.0 scores
                # in testing. _containment_ratio already covers descriptions
                # properly (requires one contiguous run, not scattered hits).
                self._token_overlap_ratio(query_tokens, self._tokenize(title)),
            )
            status_name = self._status_name(it)
            is_closed = bool(status_name) and status_name.strip().lower() in self.CLOSED_STATUS_NAMES
            scored.append((it.id, score, status_name, is_closed))
        logger.debug("Scores for query '%s': %s", original, scored)

        scored.sort(key=lambda t: t[1], reverse=True)
        eligible = [c for c in scored if c[1] >= self.MIN_MATCH_SCORE]
        if not eligible:
            logger.debug(
                "No candidate for '%s' reached MIN_MATCH_SCORE=%.02f", original, self.MIN_MATCH_SCORE
            )
            return None

        for candidate in eligible:
            if not candidate[3]:  # not closed — take the first (highest-scoring) open one
                return candidate
        # every eligible candidate is closed — surface the best of them
        return eligible[0]

    @classmethod
    def _strip_boilerplate(cls, text: str) -> str:
        return cls.BOILERPLATE_RE.sub("", text).strip(" '\"")

    @staticmethod
    def _containment_ratio(needle: str, haystack: str) -> float:
        """How much of `needle` is found as one contiguous run inside
        `haystack`, as a 0..1 fraction of `needle`'s length. Unlike
        SequenceMatcher.ratio(), this isn't penalized by `haystack` being
        much longer — an exact substring match scores 1.0 regardless of
        how much other text surrounds it."""
        # a short needle can trivially turn up as a substring of a long,
        # unrelated haystack by chance — require enough length for a
        # containment match to actually mean something.
        if len(needle) < 15:
            return 0.0
        # autojunk=False: SequenceMatcher's default autojunk=True starts
        # treating frequently-occurring characters as "junk" for sequences
        # over 200 elements, which can fragment an otherwise-clean
        # substring match — descriptions here are often long pasted logs,
        # well past that threshold.
        match = SequenceMatcher(None, needle, haystack, autojunk=False).find_longest_match(
            0, len(needle), 0, len(haystack)
        )
        return match.size / len(needle)

    @classmethod
    def _tokenize(cls, text: str) -> frozenset:
        """Lowercase word-ish tokens with STOPWORDS and very short/generic
        tokens dropped, so scoring weighs distinctive words (nvme, mkfs,
        ceph-authtool) instead of words nearly every failure shares."""
        tokens = cls._TOKEN_RE.findall(text.lower())
        return frozenset(t for t in tokens if len(t) > 2 and t not in cls.STOPWORDS)

    @staticmethod
    def _token_overlap_ratio(a: frozenset, b: frozenset) -> float:
        """Overlap coefficient (not Jaccard): |intersection| / min(|a|,|b|).
        Jaccard is penalized by the union growing with an unrelated but
        much larger `b` (e.g. a long description) — overlap coefficient
        only asks "does the smaller side's content show up in the
        other," which fits comparing a short query against a long
        description the same way _containment_ratio does for substrings."""
        if not a or not b:
            return 0.0
        return len(a & b) / min(len(a), len(b))

    @staticmethod
    def _status_name(issue_obj: Any) -> Optional[str]:
        """Best-effort read of an issue's status name. Redmine search
        results don't always carry status, and python-redmine resources
        can lazily fetch (and fail) on attribute access — never let this
        break the match, just treat status as unknown."""
        try:
            status = getattr(issue_obj, "status", None)
            return getattr(status, "name", None) if status is not None else None
        except Exception:
            return None

    # text utilities ----------------------------------------------------
    def _normalize_for_search(self, reason: str) -> str:
        """Best‑effort clean‑up so we hit the important *words* only."""
        # strip surrounding quotes
        reason = reason.strip().lstrip("'\"").rstrip("'\"")

        # 1) remove leading ISO timestamp
        reason = re.sub(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.+-]+\s+", "", reason)

        # 2) remove daemon / host identifiers (osd.12, mon.foo123, mds.a, mgr.x)
        # also remove any (){}[] around them
        reason = re.sub(r"\b(?:osd|mon|mgr|mds)\.[A-Za-z0-9_-]+\b", "", reason)
        reason = re.sub(r"[(){}]", "", reason)
        logger.debug("Reason after removing daemon IDs: %s", reason)

        # 2b) remove per-job smithi host identifiers (smithi000, smithi123 …)
        reason = re.sub(r"\bsmithi\d+\b", "", reason)

        # 2c) collapse the "mkdir -p -- <path> && cd -- <path> && " job-setup
        # prefix teuthology sometimes omits/includes, and normalise the
        # --cluster arg quoting (`--cluster ceph` vs `--cluster=ceph`) —
        # both are cosmetic differences between otherwise identical failures.
        reason = re.sub(r"mkdir -p -- \S+ && ", "", reason)
        reason = re.sub(r"--cluster=(\S+)", r"--cluster \1", reason)

        # 2d) remove the per-message sequence number cluster log lines
        # carry (e.g. "mon.a (mon.0) 3896 : cluster [ERR] ...") — like
        # smithi000 above, this number is unique to each occurrence and
        # never reproducible, so leaving it in both pollutes the cache key
        # (identical health checks get separate entries) and, worse, the
        # Redmine search query itself (all_words=True then requires that
        # exact random number appear in a ticket's text, which it never
        # will, needlessly excluding the real match).
        reason = re.sub(r"\b\d+\s*(?=:?\s*cluster\b)", "", reason, flags=re.IGNORECASE)

        # 3) remove bracketed log level tags like [WRN] [ERR] …
        #reason = re.sub(r"\[[A-Z]{3}\]", "", reason)

        # 4) scrub *all* standalone numbers (int or float) – usually not helpful
        #reason = re.sub(r"\b\d+(?:\.\d+)?\b", "", reason)

        # 5) zap stray punctuation that only creates tokens (parentheses, colons)
        #
        # NOTE: this used to be r"[()@:;\\\[\\]]", which looks like it covers
        # ( ) @ : ; [ ] \ but doesn't: inside a character class, the escaped
        # \[ and \\ pairs consume characters such that the class actually
        # closes one bracket early, leaving a dangling literal "]" the
        # pattern then requires immediately after a match. Since that never
        # occurs in practice, the substitution was a silent no-op — none of
        # : ; @ [ ] \ were ever actually stripped. Put the literal "]" right
        # after the opening "[" (where it's always literal, no escaping
        # needed) so the class can't misparse this way again.
        reason = re.sub(r"[]()@:;\[\\]", " ", reason)

        # 6) collapse whitespace
        reason = re.sub(r"\s+", " ", reason).strip()

        logger.debug("Normalized reason: %s", reason)
        return reason

    @staticmethod
    def _trim_after_colon(text: str) -> str:
        return text.split(":", 1)[1].strip() if ":" in text else text


# quick manual check ----------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(message)s")
    conn = RedmineConnector()
    sample = (
        "2025-05-18T23:33:40.185645+0000 osd.6 (osd.6) 3 : cluster [WRN] OSD bench "
        "result of 999.270273 IOPS is not within the threshold limit range of "
        "1000.000000 IOPS and 80000.000000 IOPS for osd.6. IOPS capacity is "
        "unchanged at 21500.000000 IOPS. The recommendation is to establish the "
        "osd's IOPS capacity using other benchmark tools (e.g. Fio) and then "
        "override osd_mclock_max_capacity_iops_[hdd|ssd]."
    )
    print(conn.search_and_refine(sample))
