import sqlite3
from typing import Optional, Any
from pathlib import Path
from typing import List, Dict
from .failure_scanner import FailureRecord
import logging

log = logging.getLogger(__name__)

class FailureStorage:
    """
    Persists failures and fetches aggregated stats.
    """
    def __init__(self, db_path: Path) -> None:
        # Initialize with path to SQLite DB
        self.db_path = Path(db_path)
        self.conn: Optional[sqlite3.Connection] = None

    def setup(self) -> None:
        """Create the failures table if it doesn't exist."""
        self.conn = sqlite3.connect(self.db_path)
        cur = self.conn.cursor()
        cur.execute(
            '''
            CREATE TABLE IF NOT EXISTS failures (
                id       INTEGER PRIMARY KEY,
                directory TEXT,
                version   TEXT,
                flavor    TEXT,
                date TEXT,
                reason TEXT,
                job_id TEXT
            )
            '''
        )
        self.conn.commit()

    def save(self, records: List[FailureRecord]) -> None:
        """Bulk-insert a list of FailureRecord into the DB."""
        if not self.conn:
            raise RuntimeError("Database not initialized. Call setup() first.")
        cur = self.conn.cursor()
        for rec in records:
            cur.execute(
                '''
                INSERT INTO failures
                  (directory, version, flavor, date, reason, job_id)
                VALUES (?,       ?,       ?,      ?,    ?,      ?)
                ''',
                (rec.directory,
                 rec.version,
                 rec.flavor,
                 rec.date,
                 rec.reason,
                 rec.job_id)
            )
        self.conn.commit()

    def fetch_statistics(
        self,
        version: Optional[str] = None,
        flavor: Optional[str] = None,
        since_days: Optional[int] = None,
        error_msg: Optional[str] = None,
        top_n: int = 10,
    ) -> Dict[str, int]:
        """
        Retrieve the top failure reasons, filtered by optional version, flavor,
        date range (since_days), or containing error_msg.
        """
        if not self.conn:
            raise RuntimeError("Database not initialized. Call setup() first.")
        cur = self.conn.cursor()
        clauses: List[str] = []
        params: List[Any] = []

        if version:
            clauses.append("version = ?")
            params.append(version)
        if flavor:
            clauses.append("flavor = ?")
            params.append(flavor)
        if since_days:
            # date stored as 'YYYY-MM-DD', use SQLite date functions
            clauses.append("date >= date('now', ?)")
            params.append(f"-{since_days} days")
        if error_msg:
            clauses.append("reason LIKE ?")
            params.append(f"%{error_msg}%")

        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ''
        query = (
            f"SELECT reason, COUNT(*) FROM failures "
            f"{'WHERE ' + ' AND '.join(clauses) if clauses else ''} "
            "GROUP BY reason ORDER BY COUNT(*) DESC LIMIT ?"
        )
        log.debug("----- Executing query: %s", query)
        params.append(top_n)
        log.debug("----- With parameters: %s", params)
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
        return {reason: count for reason, count in rows}
