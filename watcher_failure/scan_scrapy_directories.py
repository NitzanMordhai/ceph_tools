# scan_scrapy_directories.py
import os
import re
import datetime
import logging
from pathlib import Path
from typing import List, Union

logger = logging.getLogger(__name__)

DATE_FMT = "%Y-%m-%d"


import os
import re
import datetime
from pathlib import Path
from typing import List, Union

# make sure you have this somewhere
DATE_FMT = "%Y-%m-%d"

def list_dir_names(log_directory: str) -> List[str]:
    """One `os.scandir()` pass over `log_directory`, returning the names of
    its immediate subdirectories.

    Callers that need to filter the same base directory multiple times
    (e.g. once per version/flavor combo) should call this once and pass
    the result to `scan_scrapy_directories(..., dir_names=...)` instead of
    letting each call re-walk the directory itself — on a large, flat,
    NFS-mounted archive that directory listing is the expensive part, not
    the regex filtering.
    """
    names: List[str] = []
    try:
        for entry in os.scandir(log_directory):
            if entry.is_dir():
                names.append(entry.name)
    except FileNotFoundError:
        logging.error("Log directory not found: %s", log_directory)
    except Exception:
        logging.exception("Error listing directories under %s", log_directory)
    return names


def scan_scrapy_directories(
    log_directory: str,
    start_date: datetime.date,
    end_date: datetime.date,
    user_name: Union[str, List[str]],
    suite_name: str,
    version: str,
    branch_name: str,
    flavor: str,
    verbose: bool = False,
    db_name: str = None,
    dir_names: List[str] = None,
    **kwargs,
) -> List[str]:
    """
    Return list of directories under `log_directory` matching the pattern:
      {user_name}-{YYYY-MM-DD_time}-{suite}{...}-{version}(-release)?-distro-{flavor}-smithi
    where `user_name` can be:
      - a single string,
      - a list of strings, or
      - '*' (or ['*']) to match any user.
    Only directories whose date falls within [start_date, end_date] (inclusive) are returned.

    `dir_names`, if given, is used instead of re-scanning `log_directory` —
    pass the result of `list_dir_names(log_directory)` when filtering the
    same base directory for several version/flavor combos, to avoid
    walking it once per combo.
    """
    base = Path(log_directory)

    # --- build the “user” part of the regex ---
    if isinstance(user_name, (list, tuple)):
        # ['*'] → wildcard, otherwise an alternation of literal names
        if "*" in user_name:
            users_pat = r"[^-]+"             # “one or more non‐dash chars”
        else:
            users_pat = "(?:" + "|".join(re.escape(u) for u in user_name) + ")"
    else:
        if user_name == "*":
            users_pat = r"[^-]+"
        else:
            users_pat = re.escape(user_name)

    # suite name verbatim
    suite_pat = re.escape(suite_name)
    # allow optional “-branch” bits after suite (you can tighten this up if you know branch_name)
    branch_pat = r"(?:[:\-][^-]+)*"

    # extract the date from the directory name
    date_re = r"(?P<date>\d{4}-\d{2}-\d{2})_[0-9]{2}:[0-9]{2}:[0-9]{2}"

    if version == "main":
        # we expect nothing before -distro- (only optional branch name before)
        pattern = (
            rf"^{users_pat}-"
            rf"{date_re}-"
            rf"{suite_pat}(?:-[^-]+)*"
            rf"(?=-distro-)"                   # Lookahead for -distro-
            rf"(?:-release)?-distro-"
            rf"{re.escape(flavor)}-(?:smithi|trial)$"
        )
    else:
        # we expect explicit -version before -distro-
        pattern = (
            rf"^{users_pat}-"
            rf"{date_re}-"
            rf"{suite_pat}(?:-[^-]+)*"
            rf"-{re.escape(version)}"
            rf"(?:-release)?-distro-"
            rf"{re.escape(flavor)}-(?:smithi|trial)$"
        )

    regex = re.compile(pattern)
    if verbose:
        logging.debug("Using directory regex: %s", pattern)

    results: List[str] = []
    try:
        names = dir_names if dir_names is not None else list_dir_names(base)
        for name in names:
            m = regex.match(name)
            if not m:
                logging.debug("Skipping %s: does not match pattern %s", name, pattern)
                if name.startswith('skanta-2025-05-22'):
                    logging.debug("Skipping %s: does not match regex %s", name, pattern)
                continue
            logging.debug("Directory %s matched regex", name)
            if version == "main":
                name_before_distro = name.split("-distro-")[0]
                contains_known_version = any(f"-{ver}" in name_before_distro for ver in ["reef", "tentacle", "quincy", "squid"])
                if contains_known_version:
                    if verbose and name.startswith('skanta-2025'):
                            logging.debug("Skipping %s: contains known version (main mode)", name)
                    continue

            # check date cutoff
            date_str = m.group("date")
            try:
                d = datetime.datetime.strptime(date_str, DATE_FMT).date()
            except ValueError:
                if verbose:
                    logging.debug("Skipping %s: bad date %r", name, date_str)
                continue
            if d < start_date or d > end_date:
                if verbose:
                    logging.debug("Skipping %s: %s outside window [%s, %s]", name, d, start_date, end_date)
                continue

            full = str(base / name)
            if verbose:
                logging.debug("Accepting directory: %s", full)
            results.append(full)

    except Exception:
        logging.exception("Error scanning directories")

    return results
