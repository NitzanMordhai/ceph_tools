import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class Cleaner:
    """
    Removes DB files and images based on config.
    """
    def __init__(self, cfg) -> None:
        self.cfg = cfg

    def run(self) -> None:
        db = Path(self.cfg.db_name)
        if db.exists() and not self.cfg.keep_db:
            logger.debug("Removing database file: %s", db)
            db.unlink()
        # Remove any generated images in output_dir
        out = Path(self.cfg.output_dir)
        for img in out.glob("*_failure_statistics.png"):
            logger.debug("Removing image file: %s", img)
            img.unlink()