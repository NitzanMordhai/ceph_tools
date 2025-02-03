import requests
import os
import json
import glob
import yaml
import datetime
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_teuthology_json_output(results_path):
    # Number of generations to go back
    n = 9
    #print ("result path before %s" % results_path)
    # Get parent directory path n times
    for i in range(n):
        parent_dir_path = os.path.dirname(results_path)
        if 'remote' not in parent_dir_path:
            break
        results_path = parent_dir_path
    with open(os.path.join(parent_dir_path, 'orig.config.yaml'), 'r') as f:
        json_output = json.dumps(yaml.safe_load(f))
    
    return json_output
    
def read_total_cpu_cycles( testdir):
        pattern = '*/perf_stat.*'
        cpu_cycles_paths = glob.glob(testdir + "/" + pattern, recursive=True)
        
        total_cpu_cycles = 0
        for cpu_cycles_path in cpu_cycles_paths:
            if not cpu_cycles_path:
                continue
            with open(cpu_cycles_path, 'r') as f:
                match = re.search(r'(.*) cycles(.*?) .*', f.read())
                if not match:
                    continue

                cpu_cycles = match.group(1).strip()
                total_cpu_cycles = total_cpu_cycles + int(cpu_cycles.replace(',', ''))

        #print('total cpu cycles: %d' % total_cpu_cycles)
        return total_cpu_cycles

def load_benchmark_data(path):
    try:
        with open(path, 'r') as f:
            benchmark_data = json.load(f)
    except json.JSONDecodeError as e:
        print("Error loading JSON data from file '%s" % e)
        return None
    except FileNotFoundError as e:
        print("Error loading JSON data from file '%s" % e)
        return None
    else:
        return benchmark_data


def insert_data_into_db(path, json_output, benchmark_data):
    # Set the database connection details (change these to suit your database)
    api_url = "http://mira118.front.sepia.ceph.com:4000"
    schema_name = "public"
    table_name = "cbt_performance"
    user_name = "postgres"
    password = "root"

    # Construct the endpoint URL
    endpoint_url = f"{api_url}/{table_name}"

    # Set the authentication credentials
    auth = ("postgres", "root")

    # Set the HTTP headers
    headers = {
        "Content-Type": "application/json"
    }
    json_output = json.loads(json_output)


    job_id = json_output["job_id"]
    #data_delete = {
    #    "job_id" : job_id
    #}
    #try:
    #    response = requests.delete(endpoint_url, json=data_delete)
    #    print(f"data_delete: {data_delete}")
    #    if response.status_code == 204:
    #        deleted_count = response.headers 
    #        print(f'{deleted_count} Entry deleted successfully.')
    #    else:
    #        print(f'Failed to delete entry. Status code: {response.status_code}')
    #except requests.exceptions.RequestException as e:
    #    print(f'An error occurred: {e}')

    description = json_output["description"]
    crimson_run = False
    if description.startswith("crimson-rados"):
       crimson_run = True
    #print(f"description:{description}")

    name   = json_output["name"]
    timestamp_regex = r"\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2}"
    match = re.findall(timestamp_regex, name)
    if match:
        timestamp_str = match[0]
        timestamp = datetime.datetime.strptime(timestamp_str, "%Y-%m-%d_%H:%M:%S")
    else:
        print("No timestamp found in input string")

    started_at = timestamp
    #started_at = json_output["timestamp"]   we not always had timestamp in config.yaml
    Benchmark_mode = ""
    seq = 0
    path_full = Path(path)
    match = re.search(r'/json_output\.(?P<json>\d+)', path)
    if match:
        Benchmark_mode = path_full.parent.name if path_full.parent.name in ['rand', 'write', 'seq'] else 'fio'
        seq = match.group('json')
    else:
        print("No Benchmark_mode found in input string")
        exit(1)

    branch = json_output['branch']
    os_type = json_output['os_type']
    os_version = json_output['os_version']
    sha1 = json_output['sha1']
    machine_type = json_output['machine_type']
    tasks = json_output['tasks']
    for d in tasks:
        if "cbt" in d:
            cbt_dict = d["cbt"]
            break

    benchmark = cbt_dict["benchmarks"]
    cbt_results = benchmark_data

    data = {
        "job_id" : job_id,
        "crimson_run" : crimson_run,
        "started_at" : started_at.strftime('%Y-%m-%d %H:%M:%S'),
        "branch" : branch,
        "sha1" : sha1,
        "os_type" : os_type,
        "os_version" : os_version,
        "machine_type" : machine_type,
        "benchmark_mode" : Benchmark_mode,
        "total_cpu_cycles" : read_total_cpu_cycles(testdir=os.path.dirname(path)),
        "seq" : seq,
        "benchmark" : benchmark,
        "results" : cbt_results
    }
    #print("Inserting data into table %s" % data)
    
    response = requests.post(endpoint_url, json=data, headers=headers,  auth=auth)

    # Check the response status code
    if response.status_code != 201:
        print(f"Error inserting data: {response}")
        print (response.text)
        print (response.status_code)
        print (response.headers)
        print (response.content)
        print ("data: %s" % data)
        exit(1)
        
def find_matching_files(root_dir, pattern):
    matching_files = []
    for entry in os.scandir(root_dir):
        if entry.is_file() and glob.fnmatch.fnmatch(entry.name, pattern):
            matching_files.append(entry.path)
        elif entry.is_dir():
            matching_files.extend(find_matching_files(entry.path, pattern))
    return matching_files

def process_match(match):
    # Assuming insert_data_into_db returns some status or result
    return insert_data_into_db(
        match, 
        get_teuthology_json_output(match), 
        load_benchmark_data(match)
    )

pattern = '/a/*rados:perf*/**/json_output.*.smithi*.front.sepia.ceph.com'     
#pattern = '/a/*crimson-rados*/**/cbt/results/**/json_output.*.smithi*.front.sepia.ceph.com'
print(f"pattern: {pattern}")
matches = glob.glob(pattern, recursive=True)
print(f"number of matches: {len(matches)}")
limit = 10

with ThreadPoolExecutor(max_workers=55) as executor:
    futures = [executor.submit(insert_data_into_db, match, get_teuthology_json_output(match), load_benchmark_data(match))
        for match in matches]

    # Optional: Wait for tasks to complete
    for future in as_completed(futures):
        try:
            # Access the result of each task, even if insert_data_into_db returns None
            result = future.result()
        except Exception as e:
            print(f"An error occurred: {e}")
