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

    pattern = os.path.join(log_directory, f'{user_name}-*-{suite_name}-{branch_name}-distro-{flavor}-smithi')
    regex_pattern = fr'{user_name}-(\d{{4}}-\d{{2}}-\d{{2}})_(\d{{2}}:\d{{2}}:\d{{2}})-{suite_name}-{branch_name}-distro-{flavor}-smithi'

    directories = glob.glob(pattern)
    print(f"Found {len(directories)} directories matching the pattern: {pattern}")
    dir_results = []
    for dir_path in directories:
        print_msg(f"Processing {dir_path}", verbose)
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
    parser.add_argument('--verbose', help='Print verbose output', action='store_true')
    args = parser.parse_args()

    main(args.log_directory, args.days, args.db_name, args.user_name, args.suite_name, args.branch_name, args.flavor)
