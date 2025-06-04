from .failure_scanner import FailureRecord
from datetime import date, timedelta
from typing import Dict, Any, List, Tuple
import logging

log = logging.getLogger(__name__)

class ReportBuilder:
    """
    Takes raw stats and conversion/tracker data,
    builds an email-friendly subject, body, and inline images mapping.
    """
    def __init__(self, cfg) -> None:
        self.cfg = cfg
        # RedmineConnector for mapping reasons to issue links
        log.debug("tracker_cache_file: %s", cfg.tracker_cache_file)
        from .trackers import RedmineConnector
        self.connector = RedmineConnector(
            config_path=cfg.redmine_config_path,
            cache_file=cfg.tracker_cache_file,
        )

    def build(
        self,
        stats_by_vf: Dict[str, Dict[str, Dict[str,int]]],
        scanned_dirs: Dict[str,Dict[str,List[str]]],
        records: List[FailureRecord],
    ) -> Tuple[str,str,Dict[str,str]]:
        """
        Build email subject, body text, and image CID mapping.
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=self.cfg.days)
        subject = f"Failure Statistics Report for {start_date} to {end_date}"
        
        # Header lines
        lines = [
            f"Report for {self.cfg.user_name} (suite: {self.cfg.suite_name})",
            f"Date range: {start_date} → {end_date}",
            "",
        ]

        # 2) single‐dir mode?
        if not self.cfg.bot:
            job_ids_by_reason: Dict[str,List[str]] = {}
            for rec in records:
                job_ids_by_reason.setdefault(rec.reason, []).append(rec.job_id)

            # there should only be one key in scanned_dirs
            subject = f"Report for directory: {self.cfg.log_directory}"
            dir_key, flavor_map = next(iter(scanned_dirs.items()))
            dirs = flavor_map.get(self.cfg.flavor, [])

            # **flatten** stats if it came back nested
            # stats might be { dir_key: { flavor: {reason:count} } }
            if dir_key in stats_by_vf and isinstance(stats_by_vf[dir_key], dict):
                flat = stats_by_vf[dir_key].get(self.cfg.flavor, {})
            else:
                # maybe you already got a flat {reason:count}
                flat = stats_by_vf or {}

            subject = f"Report for directory: {dirs[0]}"  # or however you like
            lines = [
                f"Report for {self.cfg.user_name} (suite: {self.cfg.suite_name})",
                f"Date range: {start_date} → {end_date}",
                "",
                "Directories scanned:",
            ]
            for d in dirs:
                lines.append(f"  • {d}")

            lines.append("")
            lines.append("Top failures:")
            if flat:
                for idx, (reason, cnt) in enumerate(flat.items(), start=1):
                    issue = self.connector.search_and_refine(reason)
                    link = issue.get("link") or f"Issue {issue.get('issue_id','')}"
                    lines.append(f"  {idx}. {reason} ({cnt}) → {link}")
                    ids = job_ids_by_reason.get(reason, [])
                    if ids:
                        lines.append(f"     Job IDs: {ids}")
            else:
                lines.append("  (no failures found)")

            return subject, "\n".join(lines), {}

        # 3) bot (tree) mode
        else:
            log.debug("Building report for bot mode scanned directories %s", scanned_dirs)
            for version in self.cfg.versions:
                # figure out if any flavor under this version has data
                version_lines: List[str] = []
                for flavor in self.cfg.flavors:
                    log.debug("Processing version %s, flavor %s", version, flavor)
                    dirs = scanned_dirs.get(version, {}).get(flavor, [])
                    log.debug("Directories for %s/%s: %s", version, flavor, dirs)
                    failures = stats_by_vf.get(version, {}).get(flavor, {})
                    log.debug("Failures for %s/%s: %s", version, flavor, failures)
                    log.debug("stats_by_vf: %s", stats_by_vf)
                    if not dirs and not failures:
                        continue      # skip this flavor entirely

                    # we've got something—emit the flavor header
                    version_lines.append(f"-- Flavor: {flavor}")

                    if dirs:
                        version_lines.append("   Directories scanned:")
                        for d in dirs:
                            version_lines.append(f"     • {d}")
                    else:
                        version_lines.append("   (no directories scanned)")

                    if failures:
                        version_lines.append("   Top failures:")
                        top10 = sorted(failures.items(), key=lambda x: -x[1])[:10]
                        for i,(reason,count) in enumerate(top10, start=1):
                            issue = self.connector.search_and_refine(reason)
                            link  = issue.get("link") or f"Issue {issue.get('issue_id','?')}"
                            version_lines.append(f"     {i}. {reason} ({count}) → {link}")
                    else:
                        version_lines.append("   Top failures:")
                        version_lines.append("     (no failures found)")

                    version_lines.append("")  # blank between flavors

                if version_lines:
                    # only print the version header if we had at least one flavor
                    lines.append(f"=== Version: {version} ===")
                    lines.extend(version_lines)

        body = "\n".join(lines)
        return subject, body, {}