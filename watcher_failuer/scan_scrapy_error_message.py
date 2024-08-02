import os
import re
import datetime
import subprocess
import glob
import argparse

def scan_directories_for_error_message(log_directory, date, db_name)
    today = datetime.date.today()
    start_date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    arg_date = datetime.datetime.strptime(date, '%Y-%m-%d').date()

    pattern = os.path.join(log_directory, 'yuriw-*-rados-wip-*-testing-*-*-distro-default-smithi')
    directories = glob.glob(pattern)
    print(f"Found {len(directories)} directories matching the pattern.")
    
    for dir_path in directories:
        match = re.search(r'yuriw-(\d{4}-\d{2}-\d{2})_(\d{2}:\d{2}:\d{2})-rados-wip-\w+-testing-(\d{4}-\d{2}-\d{2})-\d{4}-\w+-distro-default-smithi', dir_path)
        print(f"checking {dir_path}")
        if match:
            dir_date_str = match.group(1)
            dir_time_str = match.group(2)
            scrape_date_str = match.group(3)
            dir_date = datetime.datetime.strptime(f"{dir_date_str}_{dir_time_str}", '%Y-%m-%d_%H:%M:%S').date()
            
            ## check the date, if it is in the range, then process the log
            if arg_date < dir_date:
                print(f"Date is out of range: {dir_date_str} < {date}")
                continue
            log_directory_path = os.path.join(dir_path, 'scrape.log')
            if os.path.exists(log_directory_path):
                print(f"Processing {dir_path}")
                try:
                    result = subprocess.run(
                        ['python', 'scan_scrpy.py', '--db_name', db_name, '--log_directory', dir_path, '--error_message', args.error_message],
                        text=True,
                        check=True  # Ensure subprocess raises an exception on error
                    )
                    print(f"Successfully processed {log_directory_path}")
                except subprocess.CalledProcessError as e:
                    print(f"Error processing {log_directory_path}: {e.stderr}")
            else:
                print(f"No scrape.log found in {dir_path}")

# getting the error message and max date to browse back
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scan directories and process logs.')
    parser.add_argument('--error_message', type=str, help='Error message to search for', required=True)
    parser.add_argument('--date', type=str, help='Date to scan back to', required=True)
    parser.add_argument('--db_name', type=str, help='Name of the database', required=True)
    args = parser.parse_args()

    scan_directories_for_error_message(args.error_message, args.date, args.db_name)