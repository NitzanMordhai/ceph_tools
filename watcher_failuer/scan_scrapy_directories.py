import os
import re
import datetime
import glob
import argparse
from scan_scrpy import main as scan_scrpy


def main(log_directory, days_to_scan, db_name, user_name, suite_name, branch_name, flavor, verbose=False):

    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=days_to_scan)
    #adding year to the pattern
    year = datetime.datetime.now().year
    if branch_name == 'main' and flavor == 'default':
        # For main/default, there's an extra wildcard segment (e.g., "wip-…-testing-…")
        pattern = os.path.join(log_directory, f'{user_name}-2025-*-{suite_name}-*-distro-{flavor}-smithi')
        regex_str = (
            r"^/a/[^-]+-"                               # username
            r"2025-[0-9]{2}-[0-9]{2}_[0-9]{2}:[0-9]{2}:[0-9]{2}-"  # timestamp
            r"(?:rados|crimson)-"                        # either 'rados' or 'crimson'
            r"wip-[^-]+-testing-"                        # fixed part for main/default
            r"2025-[0-9]{2}-[0-9]{2}-[0-9]{4}-"           # build timestamp segment
            r"distro-" + re.escape(flavor) + r"-smithi$"   # distro segment
        )
    elif branch_name == 'main' and flavor == 'crimson':
        # For main/crimson, the directory naming is different:
        # It places the flavor (crimson) immediately after the timestamp.
        pattern = os.path.join(log_directory, f'{user_name}-2025-*-{flavor}-{suite_name}-{branch_name}-distro-{flavor}-smithi')
        regex_str = (
            r"^/a/[^-]+-"                              # username
            r"2025-[0-9]{2}-[0-9]{2}_[0-9]{2}:[0-9]{2}:[0-9]{2}-"  # timestamp
            + re.escape(flavor) + r"-"                   # literal flavor (crimson) right after timestamp
            + re.escape(suite_name) + r"-"               # suite name (rados)
            + re.escape(branch_name) + r"-"              # branch name (main)
            r"distro-" + re.escape(flavor) + r"-smithi$"  # distro segment with flavor
        )
    else:
        # For any branch that is not "main" (like squid, reef, quincy),
        # the directory naming is simpler: it goes directly
        # from the timestamp to the suite, then the branch, then distro.
        pattern = os.path.join(log_directory, f'{user_name}-{year}-*-{suite_name}-{branch_name}-distro-{flavor}-smithi')
        regex_str = (
            r"^/a/[^-]+-"                                  # username
            r"2025-[0-9]{2}-[0-9]{2}_[0-9]{2}:[0-9]{2}:[0-9]{2}-"  # timestamp
            r"(?:rados|crimson)-"                           # suite name
            + re.escape(branch_name) + r"-"                 # branch name directly after suite name
            r"distro-" + re.escape(flavor) + r"-smithi$"      # distro segment
        )
    directories = glob.glob(pattern)
    print(f"pattern: {pattern}")
    print(f"regex_str: {regex_str}")
    regex = re.compile(regex_str)
    filtered_dirs = [d for d in directories if regex.search(d)]

    dir_results = []
    for dir_path in filtered_dirs:
        print(f"Processing {dir_path}", verbose)
        parts = os.path.basename(dir_path).split('-')
        dir_date_str = parts[1] + '-' + parts[2] + '-' + parts[3].split('_')[0]
        dir_time_str = parts[3].split('_')[1]

        if dir_date_str and dir_time_str:
            scrape_date_str = dir_date_str
            
            try:
                dir_date = datetime.datetime.strptime(f"{dir_date_str}_{dir_time_str}", '%Y-%m-%d_%H:%M:%S').date()
                scrape_date = datetime.datetime.strptime(scrape_date_str, '%Y-%m-%d').date()
            except ValueError as e:
                print(f"Error parsing date/time: {e}")
                continue
            
            if start_date <= scrape_date <= today:
                log_directory_path = os.path.join(dir_path, 'scrape.log')
                if os.path.exists(log_directory_path):
                    print(f"Scanning {log_directory_path} for {db_name}")
                    result = scan_scrpy(db_name, dir_path, False, False, None)
                    dir_results.append(dir_path)
                else:
                    print(f"No scrape.log found in {dir_path}")
            else:
                print_msg(f"Skipping {dir_path} because scrape date {scrape_date} is not in the range {start_date} to {today}", verbose)
        else:
            print_msg(f"Could not parse date/time from {dir_path}", verbose)

    return dir_results

def print_msg(msg, verbose):
    if verbose:
        print(f'{timestamp()} {msg}')

def timestamp():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Example usage:
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scan directories and process logs.')
    parser.add_argument('--log_directory', type=str, help='Directory containing logs', required=True)
    parser.add_argument('--days', type=int, help='Number of days to scan back', required=True)
    parser.add_argument('--user_name', help='The user name in directories to scan', default='yuriw')
    parser.add_argument('--suite_name', help='The suite name in directories to scan', default='rados')
    parser.add_argument('--branch_name', help='The branch name in directories to scan', default='wip-yuri*-testing-*-main')
    parser.add_argument('--db_name', type=str, help='Name of the database', required=True)
    parser.add_argument('--flavor', type=str, help='The flavor in directories to scan', default='default')
    parser.add_argument('--verbose', type=bool, help='Print verbose output', default=False)
    args = parser.parse_args()

    main(args.log_directory, args.days, args.db_name, args.user_name, args.suite_name, args.branch_name, args.flavor, args.verbose)
