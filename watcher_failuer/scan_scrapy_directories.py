import os
import re
import datetime
import subprocess
import glob
import argparse

def scan_directories_and_process_logs(log_directory, days_to_scan, db_name):
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=days_to_scan)

    pattern = os.path.join(log_directory, 'teuthology-*-rados-main-distro-default-smithi')
    
    directories = glob.glob(pattern)
    print(f"Found {len(directories)} directories matching the pattern.")
    
    for dir_path in directories:
        match = re.search(r'teuthology-(\d{4}-\d{2}-\d{2})_(\d{2}:\d{2}:\d{2})-rados-main-distro-default-smithi', dir_path)
        
        if match:
            dir_date_str = match.group(1)
            dir_time_str = match.group(2)
            scrape_date_str = match.group(1)
            
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
                    try:
                        result = subprocess.run(
                            ['python', 'scan_scrpy.py', '--db_name', db_name, '--log_directory', dir_path],
                            text=True,
                            check=True  # Ensure subprocess raises an exception on error
                        )
                    except subprocess.CalledProcessError as e:
                        print(f"Error processing {log_directory_path}: {e.stderr}")
                else:
                    print(f"No scrape.log found in {dir_path}")
                # Add more logging or actions as needed

# Example usage:
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scan directories and process logs.')
    parser.add_argument('--log_directory', type=str, help='Directory containing logs', required=True)
    parser.add_argument('--days', type=int, help='Number of days to scan back', required=True)
    parser.add_argument('--db_name', type=str, help='Name of the database', required=True)
    args = parser.parse_args()

    scan_directories_and_process_logs(args.log_directory, args.days, args.db_name)
