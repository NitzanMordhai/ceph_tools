import os
import re
import sqlite3
import argparse
import json
from collections import defaultdict
from reason_conversion import reason_conversion
from pathlib import Path

date_pattern = re.compile(r'\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2}')

def convert_reason(long_reason):
    return reason_conversion.get(long_reason, long_reason)

def normalize_machine_name(log_string):
    # Replace 'smithiXXX' with 'smithi000'
    normalized_string = re.sub(r'smithi\d+', 'smithi000', log_string)
    # Replace 'CEPH_REF=xxxxxx' with 'CEPH_REF=XXXXXXXXXXXXXXXXXX'
    normalized_string = re.sub(r'CEPH_REF=[a-f0-9]+', 'CEPH_REF=XXXXXXXXXXXXXXXXXX', normalized_string)
    return normalized_string

def parse_log_file(file_path):
    failure_pattern = re.compile(r'Failure: (.+)')
    dead_pattern = re.compile(r'Dead: (.+)')
    job_pattern = re.compile(r'\d{7,}')
    failures = defaultdict(lambda: {"count": 0, "job_ids": [], "date": ""})
    current_failure = None
    with open(file_path, 'r') as file:
        for line in file:
            # check if MAX_BACKTRACE_LINES is in the line - that means we have a backtrace 
            # and we should put it in high importance
            if "MAX_BACKTRACE_LINES" in line:
                current_failure = "ONE OF THE TESTS HAVE A BACKTRACE"
                failures[current_failure]["job_ids"].append("unknown")
                failures[current_failure]["count"] += 100 # high importance

            failure_match = failure_pattern.search(line)
            if failure_match:
                print(f"    Found failure {failure_match.group(1)}")
                long_reason = normalize_machine_name(failure_match.group(1))
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
                    else:
                        # we may have too many jobs, in that case we will have something like: "171 jobs" without job ids
                        too_many_jobs = re.search(r'\d+ jobs', line)
                        if too_many_jobs and current_failure:
                            print(f"    Found too many jobs: {too_many_jobs.group(0)}")
                            #loop and insert the db
                            for i in range(int(too_many_jobs.group(0).split()[0])):
                                failures[current_failure]["job_ids"].append("unknown")
                                failures[current_failure]["count"] += 1
                                match = date_pattern.search(file_path)
                                if match:
                                    extracted_date = match.group(0).split('_')[0]
                                else:
                                    extracted_date = "unknown"
                                failures[current_failure]["date"] = extracted_date
                                print(f"    Found too many jobs: {too_many_jobs.group(0)}")

    return failures

def store_failures_in_db(db_name, failures):
    print(f"Storing {len(failures)} failures in the database {db_name}")
    try:
        path = Path(__file__).parent.absolute()
        conn = sqlite3.connect(f"{path}/{db_name}")
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

def _get_statistics(db_name):
    print(f"Fetching statistics from the database {db_name}")
    statistics = {}
    rows = []
    try:
        path = Path(__file__).parent.absolute()
        db_name = f"{path}/{db_name}"
        if not os.path.exists(db_name):
            print(f"Error: Database {db_name} does not exist")
            return {}
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        
        cursor.execute("SELECT reason, COUNT(*), directory FROM failures GROUP BY reason ORDER BY COUNT(*) DESC LIMIT 10")
        rows = cursor.fetchall()
        conn.close()
    except sqlite3.Error as e:
        print(f"Error fetching data from the database: {e}")
        conn.close()
        return {}
    except Exception as e:
        print(f"Error: {e}")
        return {}
    
    for row in rows:
        reason, count, job_ids = row
        statistics[reason] = {"count": count, "job_ids": job_ids.split(',')}
            
    return statistics

def _get_statistics_by_error_message(db_name, error_message):
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

def main(db_name, log_directory, json_out, get_statistics, error_message):
    log_directory = f"{log_directory}"

    if get_statistics:
        if error_message:
            statistics = _get_statistics_by_error_message(db_name, error_message)
        else:
            statistics = _get_statistics(db_name)

        if json_out:
            return json.dumps(statistics, indent=2)
        else:
            return statistics
    else:
        if not log_directory:
            print("Error: --log_directory is required when not using --get_statistics")
            return 1

        all_failures = defaultdict(lambda: {"count": 0, "directory": "", "job_ids": []})
        for log_file in os.listdir(log_directory):
            if log_file.endswith("scrape.log"):
                #print(f"inside Scanning {log_file} in {log_directory}")
                log_file_path = os.path.join(log_directory, log_file)
                failures = parse_log_file(log_file_path)
                if error_message is not None:
                    #print(f"Searching for error message: {error_message}")
                    for reason, data in failures.items():
                        if error_message in reason:
                            all_failures[reason]["count"] += data["count"]
                            all_failures[reason]["directory"] = log_directory
                            all_failures[reason]["job_ids"].extend(data["job_ids"])
                else:
                    for reason, data in failures.items():
                        all_failures[reason]["count"] += data["count"]
                        all_failures[reason]["directory"] = log_directory
                        all_failures[reason]["job_ids"].extend(data["job_ids"])
                        #print(f"Found {data['count']} occurrences of {reason}")

        store_failures_in_db(db_name, all_failures)
        return 0

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Scan log files and collect failure statistics.')
    parser.add_argument('--db_name', type=str, required=True, help='Name of the SQLite database.')
    parser.add_argument('--log_directory', type=str, help='Directory containing log files.')
    parser.add_argument('--json', action='store_true', help='Output statistics as JSON.')
    parser.add_argument('--get_statistics', action='store_true', help='Fetch statistics from the database.')
    parser.add_argument('--error_message', type=str, help='Error message to search for.')

    args = parser.parse_args()

    main(args.db_name, args.log_directory, args.json, args.get_statistics, args.error_message)
