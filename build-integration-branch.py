#!/usr/bin/env python3

"""
Builds integration branches by merging PRs found by numbers.

Prerequisites:
  - GitHub CLI (`gh`): https://cli.github.com/
    Then run: `gh auth login`

Usage:
  ./build-integration-branch.py --pr 1234,5678,9012
  ./build-integration-branch.py my-label --pr 1234,5678
  ./build-integration-branch.py --pr 66055,66069,66240 
        --distros "centos9 rocky10 jammy noble" 
        --archs "x86_64" 
        --branch-name "wip-rocky10-branch-of-the-day"

Cherry-pick mode (auto-detected on release branches, or use --cherry-pick):
  ./build-integration-branch.py --pr 67264 --cherry-pick
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time

TIME_FORMAT = '%Y-%m-%d-%s'
CODENAMES = 'quincy reef squid tentacle'
REPO = "ceph/ceph"
PR_FIELDS = 'number,title,url,state,headRefName,baseRefName'
MAX_RETRIES = 5
RETRY_DELAY = 3


def run(cmd, **kw):
    return subprocess.run(cmd, text=True, **kw)


def git(*args, **kw):
    return run(['git', *args], **kw)


def gh(*args):
    result = run(['gh', *args], capture_output=True)
    if result.returncode != 0:
        print(f"gh error: {result.stderr.strip()}")
        sys.exit(1)
    return json.loads(result.stdout) if result.stdout.strip() else None


def gh_text(*args):
    """Run gh command and return raw text output (not JSON)."""
    result = run(['gh', *args], capture_output=True)
    if result.returncode != 0:
        print(f"gh error: {result.stderr.strip()}")
        sys.exit(1)
    return result.stdout


def preflight():
    if git('rev-parse', '--git-dir', capture_output=True).returncode != 0:
        sys.exit("Error: Not inside a git repository.")
    if not shutil.which('gh'):
        sys.exit("Error: GitHub CLI (gh) not installed. "
                 "See https://cli.github.com/")
    if run(['gh', 'auth', 'status'], capture_output=True).returncode != 0:
        sys.exit("Error: Not authenticated. Run: gh auth login")


def get_postfix():
    postfix = "-" + time.strftime(TIME_FORMAT, time.localtime())
    branch = git('rev-parse', '--abbrev-ref', 'HEAD',
                 check=True, capture_output=True).stdout.strip()
    if branch in CODENAMES.split():
        postfix += '-' + branch
        print(f"Adding current branch name '-{branch}' as a postfix")
    return postfix


def is_release_branch(branch):
    return branch in CODENAMES.split()


def fetch_prs(label, pr_numbers, repo):
    prs, seen = [], set()

    if label:
        labeled = gh('pr', 'list', '--repo', repo, '--label', label,
                      '--json', PR_FIELDS, '--limit', '200') or []
        labeled.sort(key=lambda p: p['number'])
        print(f"--- found {len(labeled)} PRs tagged with {label}")
        for pr in labeled:
            if pr['number'] not in seen:
                seen.add(pr['number'])
                prs.append(pr)

    for num in (pr_numbers or []):
        if num in seen:
            continue
        pr = gh('pr', 'view', str(num), '--repo', repo,
                '--json', PR_FIELDS)
        if pr.get('state') not in ('OPEN', 'open'):
            print(f"Warning: PR#{num} is {pr.get('state', 'unknown')}")
        seen.add(pr['number'])
        prs.append(pr)

    return prs


def get_pr_commits(pr_number, repo):
    """Get PR commits directly from GitHub API."""
    data = gh('pr', 'view', str(pr_number), '--repo', repo,
              '--json', 'commits')
    if not data or 'commits' not in data:
        raise Exception(f"PR#{pr_number}: could not fetch commits")
    commits = [(c['oid'], c['messageHeadline']) for c in data['commits']]
    if not commits:
        raise Exception(f"PR#{pr_number}: no commits found")
    return commits


def get_pr_diff(pr_number, repo):
    """Get the PR diff as it appears on main (from GitHub)."""
    return gh_text('pr', 'diff', str(pr_number), '--repo', repo)


def parse_diff_stats(diff_text):
    """Parse a unified diff into per-file change stats.

    Returns a dict: {filename: {'added': list of added lines,
                                 'removed': list of removed lines}}
    """
    files = {}
    current_file = None

    for line in diff_text.splitlines():
        if line.startswith('diff --git'):
            parts = line.split(' b/')
            current_file = parts[-1] if len(parts) > 1 else None
            if current_file:
                files[current_file] = {'added': [], 'removed': []}
        elif current_file:
            if line.startswith('+') and not line.startswith('+++'):
                files[current_file]['added'].append(line[1:])
            elif line.startswith('-') and not line.startswith('---'):
                files[current_file]['removed'].append(line[1:])

    return files


def validate_cherry_pick(pr, repo, before_sha):
    """Validate cherry-pick result against the PR's diff on main.

    Compares files changed and the actual line-level changes.
    Returns True if valid, False if discrepancies found.
    """
    num = pr['number']
    print(f'--- pr {num} --- validating against main...')

    # Get the PR diff as it looks on main
    pr_diff = get_pr_diff(num, repo)
    pr_stats = parse_diff_stats(pr_diff)

    # Get the diff of what we cherry-picked onto the release branch
    local_diff = git('diff', before_sha, 'HEAD',
                     capture_output=True, check=True).stdout
    local_stats = parse_diff_stats(local_diff)

    pr_files = set(pr_stats.keys())
    local_files = set(local_stats.keys())
    valid = True

    # Check for missing files (in PR but not in cherry-pick)
    missing = pr_files - local_files
    if missing:
        print(f'  WARNING: files in PR but missing from cherry-pick:')
        for f in sorted(missing):
            print(f'    - {f}')
        valid = False

    # Check for extra files (in cherry-pick but not in PR)
    extra = local_files - pr_files
    if extra:
        print(f'  WARNING: files in cherry-pick but not in PR:')
        for f in sorted(extra):
            print(f'    + {f}')
        valid = False

    # Compare line-level changes for common files
    common = pr_files & local_files
    for f in sorted(common):
        pr_added = pr_stats[f]['added']
        local_added = local_stats[f]['added']
        pr_removed = pr_stats[f]['removed']
        local_removed = local_stats[f]['removed']

        if pr_added != local_added or pr_removed != local_removed:
            print(f'  WARNING: diff mismatch in {f}:')
            if pr_added != local_added:
                pr_set = set(pr_added)
                local_set = set(local_added)
                only_pr = pr_set - local_set
                only_local = local_set - pr_set
                if only_pr:
                    print(f'    additions on main but not here: '
                          f'{len(only_pr)} line(s)')
                if only_local:
                    print(f'    additions here but not on main: '
                          f'{len(only_local)} line(s)')
            if pr_removed != local_removed:
                pr_set = set(pr_removed)
                local_set = set(local_removed)
                only_pr = pr_set - local_set
                only_local = local_set - pr_set
                if only_pr:
                    print(f'    removals on main but not here: '
                          f'{len(only_pr)} line(s)')
                if only_local:
                    print(f'    removals here but not on main: '
                          f'{len(only_local)} line(s)')
            valid = False

    if valid:
        print(f'  OK: cherry-pick matches PR diff on main')
    else:
        print(f'  MISMATCH: cherry-pick differs from PR on main')
        print(f'  This may be expected if the release branch has '
              f'diverged, but review the differences above.')

    return valid


def fetch_pr_ref(pr, repo):
    """Fetch the PR ref into a local ref. Returns the local ref name."""
    num = pr['number']
    repo_url = f'https://github.com/{repo}.git'
    ref = f'refs/pull/{num}/head'
    local_ref = f'prs/{num}'
    print(f'--- pr {num} --- fetching {repo_url} {ref}')

    for attempt in range(1, MAX_RETRIES + 1):
        rc = git('fetch', repo_url, f'+{ref}:{local_ref}').returncode
        if rc == 0:
            break
        elif rc == 1:
            print(f"  retrying ({attempt}/{MAX_RETRIES})...")
            time.sleep(RETRY_DELAY)
        else:
            raise Exception(f"Fetch failed for PR#{num} (rc={rc})")
    else:
        raise Exception(f"PR#{num} failed after {MAX_RETRIES} retries")

    return local_ref


def merge_pr(pr, repo):
    num = pr['number']
    local_ref = fetch_pr_ref(pr, repo)

    rc = git('merge', '--no-ff', '--no-edit',
             '-m', f'Merge branch {local_ref}', local_ref).returncode
    if rc != 0:
        raise Exception(f"Merge conflict on PR#{num}")


def cherry_pick_pr(pr, repo):
    num = pr['number']
    commits = get_pr_commits(num, repo)

    print(f'--- pr {num} --- cherry-picking {len(commits)} commit(s):')
    for sha, subject in commits:
        print(f'    {sha[:12]} {subject}')

    for sha, subject in commits:
        rc = git('cherry-pick', sha).returncode
        if rc != 0:
            raise Exception(
                f"Cherry-pick conflict on PR#{num} commit {sha[:12]} "
                f"({subject})")

    return commits


def parse_args():
    parser = argparse.ArgumentParser(usage=__doc__)
    parser.add_argument("label", nargs='?', default=None,
                        help="GitHub label to search for")
    parser.add_argument("--pr", type=lambda v: [int(x) for x in v.split(',')],
                        default=[], help="Comma-separated PR numbers")
    parser.add_argument("--branch-name", help="Override branch name")
    parser.add_argument("--no-date", "--no-postfix", action="store_true",
                        help="Don't add date postfix to branch name")
    parser.add_argument("--repo", default=REPO)
    parser.add_argument("--dry-run", action="store_true",
                        help="Dry run mode: check what would be done "
                             "without making changes")
    parser.add_argument("--cherry-pick", action="store_true",
                        help="Cherry-pick PR commits instead of merging "
                             "(auto-enabled on release branches)")
    parser.add_argument("--merge", action="store_true",
                        help="Force merge mode even on release branches")
    parser.add_argument("--no-validate", action="store_true",
                        help="Skip validation of cherry-picks against main")
    parser.add_argument("--trailer", action="append", dest='trailers')
    parser.add_argument('--ceph-build-job', action="append", dest='trailers',
                        type=lambda v: f'CEPH-BUILD-JOB: {v}')
    parser.add_argument('--distros', action="append", dest='trailers',
                        type=lambda v: f'DISTROS: {v}')
    parser.add_argument('--archs', action="append", dest='trailers',
                        type=lambda v: f'ARCHS: {v}')
    args = parser.parse_args()
    if not args.label and not args.pr:
        parser.error("Must specify either a label or --pr")
    return args


def main():
    cli = parse_args()
    preflight()

    original_branch = git('rev-parse', '--abbrev-ref', 'HEAD',
                          check=True, capture_output=True).stdout.strip()

    # Determine merge vs cherry-pick mode
    if cli.merge:
        use_cherry_pick = False
    elif cli.cherry_pick:
        use_cherry_pick = True
    else:
        use_cherry_pick = is_release_branch(original_branch)

    if use_cherry_pick:
        print(f"--- cherry-pick mode (base branch: {original_branch})")
    else:
        print(f"--- merge mode")

    base = cli.branch_name or cli.label or 'integration'
    branch = base if cli.no_date else base + get_postfix()

    prs = fetch_prs(cli.label, cli.pr, cli.repo)
    if not prs:
        sys.exit("--- no PRs found, nothing to do")
    print(f"--- queried {len(prs)} prs")

    if cli.dry_run:
        print("--- dry-run mode: summary of changes")
        print(f"  Would create branch: {branch}")
        mode = "cherry-pick" if use_cherry_pick else "merge"
        print(f"  Would {mode} {len(prs)} PRs:")
        for pr in prs:
            print(f"    {pr['url']} - {pr['title']}")
        if cli.trailers:
            print(f"  Would add trailers: {cli.trailers}")
        print("--- no changes made")
        return

    # Assemble branch
    print(f'--- creating branch {branch}')
    git('branch', '-D', branch, capture_output=True)  # silent if missing
    if git('checkout', '-b', branch).returncode != 0:
        sys.exit(f"Failed to create branch {branch}")

    cherry_picked = {}  # {pr_number: [(sha, subject), ...]}

    try:
        for pr in prs:
            if use_cherry_pick:
                before_sha = git('rev-parse', 'HEAD',
                                 capture_output=True,
                                 check=True).stdout.strip()
                commits = cherry_pick_pr(pr, cli.repo)
                cherry_picked[pr['number']] = commits
                if not cli.no_validate:
                    validate_cherry_pick(pr, cli.repo, before_sha)
            else:
                merge_pr(pr, cli.repo)
    except Exception as e:
        print(f'--- error: {e}')
        if use_cherry_pick:
            git('cherry-pick', '--abort', capture_output=True)
        else:
            git('merge', '--abort', capture_output=True)
        git('checkout', original_branch)
        git('branch', '-D', branch, capture_output=True)
        sys.exit(1)

    refs = ', '.join(f"prs/{pr['number']}" for pr in prs)
    message = f"Merged branches {refs}\n"
    if cli.trailers:
        message += '\n' + '\n'.join(cli.trailers) + '\n'
    cmd = ['git', 'commit', '--allow-empty', '-m', message]
    if run(cmd).returncode != 0:
        sys.exit('Failed to amend final commit!')

    print()
    print('=' * 60)
    print('  SUMMARY')
    print('=' * 60)
    print(f'  Branch: {branch}')
    print(f'  Mode:   {"cherry-pick" if use_cherry_pick else "merge"}')
    print(f'  PRs:    {len(prs)}')
    print()

    for pr in prs:
        num = pr['number']
        print(f'  PR #{num} - {pr["title"]}')
        print(f'  {pr["url"]}')
        if num in cherry_picked:
            commits = cherry_picked[num]
            print(f'  cherry-picked {len(commits)} commit(s):')
            for sha, subject in commits:
                print(f'    {sha[:12]}  {subject}')
        print()

    print('=' * 60)
    print(f'  perhaps you want to:')
    print(f'  ./run-make-check.sh && git push ci {branch}')
    print('=' * 60)


if __name__ == '__main__':
    main()