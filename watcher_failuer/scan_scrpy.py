import os
import re
import sqlite3
import argparse
import json
from collections import defaultdict
from reason_conversion import reason_conversion  # Import the conversion table

def convert_reason(long_reason):
    return reason_conversion.get(long_reason, long_reason)

def parse_log_file(file_path):
    failure_pattern = re.compile(r'Failure: (.+)')
    job_pattern = re.compile(r'\d{7,}')
    failures = defaultdict(lambda: {"count": 0, "job_ids": [], "date": ""})
    current_failure = None
    with open(file_path, 'r') as file:
        for line in file:
            failure_match = failure_pattern.search(line)
            if failure_match:
                long_reason = failure_match.group(1)
                short_reason = convert_reason(long_reason)
                print(f"Converted {long_reason}")
                print(f"to {short_reason}")
                current_failure = short_reason
            else:
                job_ids = job_pattern.findall(line)
                if job_ids:
                    if current_failure:
                        failures[current_failure]["job_ids"].extend(job_ids)
                        failures[current_failure]["count"] += 1
                        failures[current_failure]["date"] = file_path.split('/')[-2]
                        print(f"found date {failures[current_failure]['date']}")

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
                job_id TEXT NOT NULL
            )
        ''')

        # Insert new failures
        for reason, data in failures.items():
            try:
                for job_id in data['job_ids']:
                    cursor.execute('''
                        INSERT INTO failures (reason, job_id)
                        VALUES (?, ?)
                    ''', (reason, job_id))
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
    cursor.execute("SELECT reason, COUNT(*), GROUP_CONCAT(job_id) FROM failures GROUP BY reason ORDER BY COUNT(*) DESC LIMIT 10")
    rows = cursor.fetchall()
    conn.close()
    
    statistics = {}
    for row in rows:
        reason, count, job_ids = row
        statistics[reason] = {"count": count, "job_ids": job_ids.split(',')}
    
    return statistics

def print_statistics(statistics):
    print("Top 10 Failure Statistics:")
    for reason, data in statistics.items():
        print(f"{reason}: {data['count']} occurrences")
        print("Jobs:")
        print(", ".join(data['job_ids']))
        print("")

def main():
    parser = argparse.ArgumentParser(description='Scan log files and collect failure statistics.')
    parser.add_argument('--db_name', type=str, required=True, help='Name of the SQLite database.')
    parser.add_argument('--log_directory', type=str, help='Directory containing log files.')
    parser.add_argument('--json', action='store_true', help='Output statistics as JSON.')
    parser.add_argument('--get_statistics', action='store_true', help='Fetch statistics from the database.')
    parser.add_argument('--error_message', type=str, help='Error message to search for.')

    args = parser.parse_args()
    if args.get_statistics:
        statistics = get_statistics(args.db_name)
        if args.json:
            print(json.dumps(statistics, indent=2))
        else:
            print_statistics(statistics)
    else:
        if not args.log_directory:
            print("Error: --log_directory is required when not using --get_statistics")
            exit(1)

        all_failures = defaultdict(lambda: {"count": 0, "job_ids": []})
        for log_file in os.listdir(args.log_directory):
            if log_file.endswith("scrape.log"):
                log_file_path = os.path.join(args.log_directory, log_file)
                failures = parse_log_file(log_file_path)
                if error_message:
                    for reason, data in failures.items():
                        if error_message in reason:
                            all_failures[reason]["count"] += data["count"]
                            all_failures[reason]["job_ids"].extend(data["job_ids"])
                else:
                    for reason, data in failures.items():
                        all_failures[reason]["count"] += data["count"]
                        all_failures[reason]["job_ids"].extend(data["job_ids"])

        store_failures_in_db(args.db_name, all_failures)
        if args.get_statistics:
            statistics = get_statistics(args.db_name)

            if args.json:
                print(json.dumps(statistics, indent=2))
            else:
                print_statistics(statistics)

if __name__ == "__main__":
    main()
