import configparser
import os
from redminelib import Redmine
from difflib import SequenceMatcher
import sys

class RedmineConnector:
    def __init__(self, config_path='~/.redmin'):
        if config_path is None:
            config_path = os.path.expanduser('~/.redmin')

        config_path = os.path.expanduser(config_path)
        self.config = self._load_config(config_path)

        self.redmine = Redmine(
            self.config['redmine']['url'],
            username=self.config['redmine']['username'],
            password=self.config['redmine']['password']
        )

        project = self.redmine.project.get(self.config['redmine']['project_name'])
        self.ceph_project_id = project.id

    def _load_config(self, path):
        config = configparser.ConfigParser()
        config.read(path)
        return config

    def search_and_refine(self, search_string, status=None, limit=50):
        # Perform search by words in the string
        if search_string is not None and not isinstance(search_string, str):
            raise ValueError("search_string must be a string")

        query_params = {
            'limit': limit,
            'all_words': True,
            'wiki_pages': 0,
            'attachments': False,
            'documents': False,
            'query': search_string
        }

        issues = self.redmine.issue.search(**query_params)
        if not issues:
            print(f"No issues found with the following words: {query_params}")
            return []

        matching_issues = [
            (issue.id, self._string_similarity(search_string, issue.description or ""))
            for issue in issues if issue.description  # Only compare if issue has a description
        ]

        sorted_matches = sorted(matching_issues, key=lambda x: x[1], reverse=True)
        best_matches_id , best_matches_score = sorted_matches[0]
        best_matched_issue = self.redmine.issue.get(best_matches_id)
        return_issue = {
            'link': self.config['redmine']['url'] + f"/issues/{best_matched_issue.id}",
            'issue_id': best_matched_issue.id,
            'description': best_matched_issue.description,
            'match_score': best_matches_score
        }
        return return_issue

    def _string_similarity(self, str1, str2):
        return SequenceMatcher(None, str1, str2).ratio()

if __name__ == "__main__":
    connector = RedmineConnector()
    search_string = "bug fix memory leak"  # Example search string
    results = connector.search_and_refine(search_string)

    if results:
        for issue_id, tracker_id, tracker_name, match_score in results:
            print(f"Issue ID: {issue_id}, Tracker ID: {tracker_id}, Name: {tracker_name}, Match: {match_score:.2%}")
    else:
        print("No matching issues found.")
