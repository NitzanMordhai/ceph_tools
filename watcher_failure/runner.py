from pathlib import Path
import logging
from .failure_scanner import FailureScanner
from .failure_storage import FailureStorage
from .report_builder import ReportBuilder
from .email_sender import EmailSender
from .cleaner import Cleaner
from pathlib import Path as _P

log = logging.getLogger(__name__)

class Runner:
    def __init__(self, cfg) -> None:
        self.cfg = cfg
        self.scanner = FailureScanner(cfg)
        self.storage = FailureStorage(Path(cfg.db_name))
        self.builder = ReportBuilder(cfg)
        self.sender  = EmailSender(cfg)
        self.cleaner = Cleaner(cfg)

    def run(self) -> None:
        log.debug("Starting run with config: %s", self.cfg)
        # 1. Setup DB
        self.storage.setup()
        # 2. Scan logs
        if self.cfg.bot:
            grouped_records, scanned_dirs = self.scanner.scan_tree()
            records = [
                rec
                for version_map in grouped_records.values()
                for rec_list in version_map.values()
                for rec in rec_list
            ]
        else:
            recs, dirs = self.scanner.scan_directory(_P(self.cfg.log_directory))
            records = recs
            key = Path(self.cfg.log_directory).name
            scanned_dirs = { key: { self.cfg.flavor: dirs } }
            log.debug("Single directory scan: %s", scanned_dirs)
            log.debug("key: %s", key)

        
        #log.debug("Scanned directories: %s", scanned_dirs)
        #log.debug("Parsed records: ")
        #for rec in records:
        #    log.debug("record:      %s", rec)

        self.storage.save(records)
        stats_by_vf: Dict[str, Dict[str, Dict[str,int]]] = {}
        if self.cfg.bot:
            # tree mode: stats per real version/flavor
            for version, flavor_map in scanned_dirs.items():
                stats_by_vf[version] = {}
                for flavor in flavor_map:
                    stats_by_vf[version][flavor] = self.storage.fetch_statistics(
                        version=version,
                        flavor=flavor,
                        since_days=self.cfg.days,
                        error_msg=self.cfg.error_message,
                        top_n=10,
                    )
        else:
            stats = self.storage.fetch_statistics(top_n=10)
            stats_by_vf[key] = {self.cfg.flavor: stats}

        log.debug("Statistics fetched: %s", stats_by_vf)
        # 4. Build report
        subject, body, images = self.builder.build(stats_by_vf, scanned_dirs, records)
        log.debug("Report built with subject: %s", subject)
        # 5. Send
        self.sender.send(subject, body, images)
        log.debug("Email sent to: %s", self.cfg.email)
        # 6. Cleanup
        self.cleaner.run()
        log.debug("Cleanup completed")