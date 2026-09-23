"""Stub out redminelib so trackers.py imports without the real package or
a network connection — every test drives RedmineConnector against fakes."""
import sys
import types


class _FakeProject:
    def __init__(self, id_):
        self.id = id_


class _FakeProjectManager:
    def get(self, name):
        return _FakeProject(id_=99)


class _FakeIssueManager:
    def search(self, **kwargs):
        return []


class _FakeRedmine:
    def __init__(self, url, username="", key=""):
        self.url = url
        self.project = _FakeProjectManager()
        self.issue = _FakeIssueManager()


if "redminelib" not in sys.modules:
    fake_module = types.ModuleType("redminelib")
    fake_module.Redmine = _FakeRedmine
    sys.modules["redminelib"] = fake_module
