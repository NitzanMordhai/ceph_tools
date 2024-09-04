import os
import re
import sqlite3
import argparse
import json
from collections import defaultdict
from reason_conversion import reason_conversion  # Import the conversion table
from pathlib import Path

path = Path(__file__).parent.absolute()
date_pattern = re.compile(r'\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2}')

def convert_reason(long_reason):
    return reason_conversion.get(long_reason, long_reason)

def parse_log_file(file_path):
    failure_pattern = re.compile(r'Failure: (.+)')
    dead_pattern = re.compile(r'Dead: (.+)')
    job_pattern = re.compile(r'\d{7,}')
    failures = defaultdict(lambda: {"count": 0, "job_ids": [], "date": ""})
    current_failure = None
    with open(file_path, 'r') as file:
        for line in file:
            failure_match = failure_pattern.search(line)
            if failure_match:
                long_reason = failure_match.group(1)
                short_reason = convert_reason(long_reason)
                current_failure = short_reason
            else:
                dead_match = dead_pattern.search(line)
                if dead_match:
                    long_reason = dead_match.group(1)
                    short_reason = convert_reason(long_reason)
                    current_failure = short_reason
                else:
                    job_ids = job_pattern.findall(line)
                    if job_ids:
                        if current_failure:
                            failures[current_failure]["job_ids"].extend(job_ids)
                            failures[current_failure]["count"] += 1
                            match = date_pattern.search(file_path)
                            if match:
                                extracted_date = match.group(0).split('_')[0]
                            else:
                                extracted_date = "unknown"
                            failures[current_failure]["date"] = extracted_date

    return failures

def store_failures_in_db(db_name, failures):
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Create the table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS failures (
                id INTEGER PRIMARY KEY,
                reason TEXT NOT NULL,
                directory TEXT NOT NULL,
                job_id TEXT NOT NULL
            )
        ''')

        # Insert new failures
        for reason, data in failures.items():
            try:
                for job_id in data['job_ids']:
                    cursor.execute('''
                        INSERT INTO failures (reason, job_id, directory)
                        VALUES (?, ?, ?)
                    ''', (reason, job_id, data['directory']))
                    conn.commit()  # Commit after inserting each reason's job_ids
                
            except sqlite3.Error as e:
                print(f"Error inserting data for {reason}: {e}")
                conn.rollback()  # Rollback transaction on error

    except sqlite3.Error as e:
        print(f"Error executing SQLite operation: {e}")

    finally:
        if conn:
            conn.close()

def get_statistics(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    # return only the top 10 reasons
    cursor.execute("SELECT reason, COUNT(*), directory FROM failures GROUP BY reason ORDER BY COUNT(*) DESC LIMIT 10")
    rows = cursor.fetchall()
    conn.close()
    
    statistics = {}
    for row in rows:
        reason, count, job_ids = row
        statistics[reason] = {"count": count, "job_ids": job_ids.split(',')}
        
    return statistics

def get_statistics_by_error_message(db_name, error_message):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT reason, GROUP_CONCAT(job_id), directory FROM failures WHERE reason LIKE ? GROUP BY reason, directory ORDER BY COUNT(*) DESC", (f"%{error_message}%",))
    rows = cursor.fetchall()
    conn.close()
    
    statistics = {}
    for row in rows:
        reason, job_ids, directory = row
        if reason not in statistics:
            statistics[reason] = {"job_ids": [], "directory": directory}
        statistics[reason]["job_ids"].extend(job_ids.split(','))

    return statistics

def print_statistics(statistics):
    print("Top 10 Failure Statistics:")
    for reason, data in statistics.items():
        print(f"{reason}: {data['count']} occurrences")
        print("Jobs:")
        print(", ".join(data['job_ids']))
        print(", ".join(data['directory']))
        print("")

def main():
    parser = argparse.ArgumentParser(description='Scan log files and collect failure statistics.')
    parser.add_argument('--db_name', type=str, required=True, help='Name of the SQLite database.')
    parser.add_argument('--log_directory', type=str, help='Directory containing log files.')
    parser.add_argument('--json', action='store_true', help='Output statistics as JSON.')
    parser.add_argument('--get_statistics', action='store_true', help='Fetch statistics from the database.')
    parser.add_argument('--error_message', type=str, help='Error message to search for.')

    args = parser.parse_args()
    db_name = f"{path}/{args.db_name}"
    log_directory = f"{path}/{args.log_directory}"

    if args.get_statistics:
        if args.error_message:
            statistics = get_statistics_by_error_message(db_name, args.error_message)
        else:
            statistics = get_statistics(db_name)
        if args.json:
            print(json.dumps(statistics, indent=2))
        else:
            print_statistics(statistics)
    else:
        if not args.log_directory:
            print("Error: --log_directory is required when not using --get_statistics")
            exit(1)

        all_failures = defaultdict(lambda: {"count": 0, "directory": "", "job_ids": []})
        for log_file in os.listdir(args.log_directory):
            if log_file.endswith("scrape.log"):
                log_file_path = os.path.join(args.log_directory, log_file)
                failures = parse_log_file(log_file_path)
                if args.error_message is not None:
                    for reason, data in failures.items():
                        if args.error_message in reason:
                            all_failures[reason]["count"] += data["count"]
                            all_failures[reason]["directory"] = args.log_directory
                            all_failures[reason]["job_ids"].extend(data["job_ids"])
                else:
                    for reason, data in failures.items():
                        all_failures[reason]["count"] += data["count"]
                        all_failures[reason]["directory"] = args.log_directory
                        all_failures[reason]["job_ids"].extend(data["job_ids"])

        store_failures_in_db(db_name, all_failures)
        if args.get_statistics:
            statistics = get_statistics(db_name)

            if args.json:
                print(json.dumps(statistics, indent=2))
            else:
                print_statistics(statistics)

if __name__ == "__main__":
    main()
