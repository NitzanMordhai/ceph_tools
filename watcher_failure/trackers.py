#!/usr/bin/env python3
"""
trackers.py

Utilities for Redmine issue caching and lookup.
Provides a connector that loads configuration from an INI file,
queries Redmine, and caches results in JSON.
"""
import configparser
import json
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

from redminelib import Redmine
from difflib import SequenceMatcher
import re

# Module-level logger
logger = logging.getLogger(__name__)

class RedmineConnector:
    """
    Connects to a Redmine server using credentials from a config file.
    Caches query results locally to reduce API calls.
    """
    def __init__(
        self,
        config_path: str = '~/.redmin',
        cache_file: str = 'tracker_cache.json'
    ) -> None:
        # Load configuration
        self.config = self._load_config(config_path)
        
        module_dir = Path(__file__).parent
        self.cache_path = module_dir / cache_file
        # Load or initialize cache
        self.cache: Dict[str, Any] = self._load_cache()

        if self.config.has_section('redmine'):
            redmine_cfg = self.config['redmine']
        else:
            redmine_cfg = {}
        
        logger.debug(f"   Redmine config: {redmine_cfg}")
        # Initialize Redmine client
        self.redmine = Redmine(
            redmine_cfg.get('url', ''),
            username=redmine_cfg.get('username', ''),
            password=redmine_cfg.get('password', '')
        )
        project_name = redmine_cfg.get('project_name', '')
        if project_name:
            project = self.redmine.project.get(project_name)
            self.ceph_project_id = project.id
            logger.debug(f"Connected to Redmine project ID: {self.ceph_project_id}")

    def _load_config(self, path: Optional[str]) -> configparser.ConfigParser:
        """
        Load INI config from `path` or ~/.redmin.
        """
        parser = configparser.ConfigParser()
        cfg_path = Path(path or '~/.redmin').expanduser()
        logger.debug(f"Loading config from {cfg_path}")
        parser.read(cfg_path)
        read_files = parser.read(cfg_path)
        logger.debug(f"Config file read: {read_files}")
        return parser

    def _load_cache(self) -> Dict[str, Any]:
        """
        Load JSON cache from disk; return empty if missing or invalid.
        """
        try:
            with open(self.cache_path, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.debug(f"Cache load failed ({e}); starting empty cache.")
            return {}

    def _save_cache(self) -> None:
        """
        Persist current cache to disk.
        """
        try:
            with open(self.cache_path, 'w') as f:
                json.dump(self.cache, f, indent=2)
            logger.debug(f"Cache saved to {self.cache_path}")
        except Exception as e:
            logger.error(f"Failed to save cache: {e}")

    def search_and_refine(
        self,
        search_string: str,
        status: Optional[str] = None,
        limit: int = 50
    ) -> Dict[str, Any]:
        """
        Search Redmine for issues matching `search_string`, cache and return top match.

        Returns a dict with 'issue_id' and 'link', or empty if none found.
        """
        if not isinstance(search_string, str):
            raise ValueError("search_string must be a string")

        # Remove known noise patterns
        exclude_pattern = r'Command failed on smithi\d+ with status \d+'
        query = re.sub(exclude_pattern, '', search_string)

        # Return cached result if available
        if query in self.cache:
            logger.info(f"Cache hit for '{query}'")
            return self.cache[query]

        # Fetch issues from Redmine
        issues = self._fetch_issues(query, status, limit)
        if not issues:
            logger.info(f"No issues found for '{query}'")
            self.cache[query] = {}
            self._save_cache()
            return {}

        # Determine best match by similarity
        best = self._find_best_match(query, issues, exclude_pattern)
        if not best:
            return {}

        issue_id, _, link = best
        result = {'issue_id': issue_id, 'link': link}
        self.cache[query] = result
        self._save_cache()
        return result

    def _fetch_issues(
        self,
        query: str,
        status: Optional[str],
        limit: int
    ) -> List[Any]:
        """
        Wrapper around Redmine issue.search with common parameters.
        """
        params = {
            'limit': limit,
            'all_words': True,
            'wiki_pages': False,
            'attachments': False,
            'open_issues': True,
            'query': query
        }
        if status:
            params['status_id'] = status
        logger.debug(f"Searching Redmine with params: {params}")
        return self.redmine.issue.search(**params)

    def _find_best_match(
        self,
        query: str,
        issues: List[Any],
        exclude_pattern: str
    ) -> Optional[Tuple[int, float, str]]:
        """
        Score issues by similarity to query; return top (id, score, link).
        """
        scores: List[Tuple[int, float, str]] = []
        for issue in issues:
            title = self._trim_after_colon(issue.title)
            desc = issue.description or ''
            cleaned = lambda s: re.sub(exclude_pattern, '', s)
            score = max(
                SequenceMatcher(None, query, cleaned(title)).ratio(),
                SequenceMatcher(None, query, cleaned(desc)).ratio()
            )
            link = f"{self.config['redmine']['url']}/issues/{issue.id}"
            scores.append((issue.id, score, link))

        best = max(scores, key=lambda x: x[1], default=None)
        logger.info(f"Best match: {best}")
        return best

    def _trim_after_colon(self, s: str) -> str:
        """
        Return substring after first colon, else original string.
        """
        return s.split(':', 1)[1].strip() if ':' in s else s

# Example usage
if __name__ == '__main__':
    connector = RedmineConnector()
    result = connector.search_and_refine('memory leak issue')
    print(result)
