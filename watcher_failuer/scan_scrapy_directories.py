import os
import re
import datetime
import glob
import argparse
from scan_scrpy import main as scan_scrpy


def main(log_directory, days_to_scan, db_name, user_name='teuthology', flavor='squid'):
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=days_to_scan)

    branch_name = 'wip-yuri*-testing-*'
    suite_name = 'rados'

    pattern = os.path.join(log_directory, f'{user_name}-*-{suite_name}-{branch_name}-{flavor}-distro-default-smithi')
    regex_pattern = fr'{user_name}-(\d{{4}}-\d{{2}}-\d{{2}})_(\d{{2}}:\d{{2}}:\d{{2}})-{suite_name}-{branch_name}-{flavor}-distro-default-smithi'
    directories = glob.glob(pattern)
    print(f"Found {len(directories)} directories matching the pattern.")
    dir_results = []
    for dir_path in directories:
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
                print(f"checking {dir_path}")
                log_directory_path = os.path.join(dir_path, 'scrape.log')
                if os.path.exists(log_directory_path):
                    result = scan_scrpy(db_name, dir_path, False, False, None)
                    dir_results.append(dir_path)
                else:
                    print(f"No scrape.log found in {dir_path}")

    for dir_result in dir_results:
        print(f" results from {dir_result}")
    return dir_results


# Example usage:
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scan directories and process logs.')
    parser.add_argument('--log_directory', type=str, help='Directory containing logs', required=True)
    parser.add_argument('--days', type=int, help='Number of days to scan back', required=True)
    parser.add_argument('--user_name', type=str, help='The user name in directories to scan', default='teuthology')
    parser.add_argument('--db_name', type=str, help='Name of the database', required=True)
    args = parser.parse_args()

    main(args.log_directory, args.days, args.db_name, args.user_name)
