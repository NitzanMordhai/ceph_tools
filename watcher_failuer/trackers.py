import configparser
import os
from redminelib import Redmine
from difflib import SequenceMatcher
import difflib
import sys
import re
from pathlib import Path
import json

class RedmineConnector:
    def __init__(self, config_path='~/.redmin', cache_file='tracker_cache.json'):
        if config_path is None:
            config_path = os.path.expanduser('~/.redmin')

        config_path = os.path.expanduser(config_path)
        self.config = self._load_config(config_path)
        path = Path(__file__).parent.absolute()
        cache_file = os.path.join(path, cache_file)
        self.cache_file = cache_file
        self.cache = self.load_cache()

        self.redmine = Redmine(
            self.config['redmine']['url'],
            username=self.config['redmine']['username'],
            password=self.config['redmine']['password']
        )

        project = self.redmine.project.get(self.config['redmine']['project_name'])
        self.ceph_project_id = project.id

    def load_cache(self):
        if os.path.exists(self.cache_file):
            with open(self.cache_file, 'r') as f:
                return json.load(f)
        return {}

    def save_cache(self):
        with open(self.cache_file, 'w') as f:
            json.dump(self.cache, f)

    def _load_config(self, path):
        config = configparser.ConfigParser()
        config.read(path)
        return config

    def search_and_refine(self, search_string, status=None, limit=50):
        # Perform search by words in the string
        if search_string is not None and not isinstance(search_string, str):
            raise ValueError("search_string must be a string")
        exclude_pattern = r'Command failed on smithi\d+ with status \d+'
        search_string = re.sub(exclude_pattern, '', search_string)
        if search_string in self.cache:
            print(f"Found in cache: {search_string}")
            return self.cache[search_string]

        query_params = {
            'limit': limit,
            'all_words': True,
            'wiki_pages': False,
            'attachments': False,
            'documents': False,
            'open_issues': True,
            'query': search_string
        }

        issues = self.redmine.issue.search(**query_params)

        if not issues:
            print(f"No issues found with the following words: {query_params}")
            self.cache[search_string] = {}
            return {}

        print(f"Found {len(issues)} issues with the following words: {query_params}")
        matching_issues = [
            (issue.id, self.calculate_similarity(search_string, re.sub(exclude_pattern, '', self.trim_after_colon(issue.title)) or ""))
            for issue in issues if issue.description
        ]

        sorted_matches = sorted(matching_issues, key=lambda x: x[1], reverse=True)

        best_matches_id , best_matches_score = sorted_matches[0]
        # if best match score is less than 0.5, we will try the description and see if we can find a better match
        if best_matches_score < 0.5:
            matching_issues_desc = [
                (issue.id, self.calculate_similarity(search_string, re.sub(exclude_pattern, '', issue.description) or ""))
                for issue in issues if issue.description
            ]
            sorted_matches_desc = sorted(matching_issues_desc, key=lambda x: x[1], reverse=True)

            best_matches_id_desc, best_matches_score_desc = sorted_matches_desc[0]
            if best_matches_score_desc > best_matches_score:
                best_matches_id = best_matches_id_desc
                best_matches_score = best_matches_score_desc

        best_matched_issue = self.redmine.issue.get(best_matches_id)
        return_issue = {
            'link': self.config['redmine']['url'] + f"/issues/{best_matched_issue.id}",
            'issue_id': best_matched_issue.id
        }

        self.cache[search_string] = return_issue
        return return_issue

    def trim_after_colon(self, s):
        return s.split(':', 1)[1] if ':' in s else s


    def calculate_similarity(self, str1, str2):
        return difflib.SequenceMatcher(None, str1, str2).ratio()

if __name__ == "__main__":
    connector = RedmineConnector()
    search_string = "bug fix memory leak"  # Example search string
    results = connector.search_and_refine(search_string)

    if results:
        for issue_id, tracker_id, tracker_name, match_score in results:
            print(f"Issue ID: {issue_id}, Tracker ID: {tracker_id}, Name: {tracker_name}, Match: {match_score:.2%}")
    else:
        print("No matching issues found.")
