import os
import random
import time
import subprocess
import argparse
import json
import signal

def get_osd_daemon_type(daemon_type):
    return "ceph-osd" if daemon_type == "classic" else "crimson-osd"

def get_live_osds():
    osd_tree = subprocess.check_output(["ceph", "osd", "tree", "-f", "json"]).decode("utf-8")
    osd_tree_json = json.loads(osd_tree)
    print(f"osd tree: {osd_tree_json}")
    live_osds = [node['id'] for node in osd_tree_json['nodes'] if node['type'] == 'osd' and node.get('status') == 'up']
    return live_osds

def kill_osd(osd_id, daemon_type):
    pid_file = f"/home/nmordech/ceph_crimson/build/out/osd.{osd_id}.pid"
    if not os.path.isfile(pid_file):
        print(f"PID file for OSD {osd_id} not found.")
        return
    with open(pid_file, 'r') as f:
        pid = int(f.read().strip())
    os.kill(pid, signal.SIGTERM)
    print(f"Killed OSD {osd_id} (PID: {pid}).")

def revive_osd(osd_id, daemon_type):
    osd_command = f"{daemon_type} -i {osd_id} &"
    subprocess.run(osd_command, shell=True)
    print(f"Revived OSD {osd_id}.")

def main(daemon_type):
    daemon_type_command = get_osd_daemon_type(daemon_type)
    live_osds = get_live_osds()
    down_osds = set()
    
    print(f"Initial live OSDs: {live_osds}")
    
    try:
        while True:
            if not live_osds:
                print("No live OSDs to kill. Exiting.")
                break

            osd_to_kill = random.choice(live_osds)
            kill_osd(osd_to_kill, daemon_type_command)
            live_osds.remove(osd_to_kill)
            down_osds.add(osd_to_kill)
            
            time.sleep(10)  # Adjust the sleep time as necessary
            
            revive_osd(osd_to_kill, daemon_type_command)
            live_osds.append(osd_to_kill)
            down_osds.remove(osd_to_kill)
            
            print(f"OSDs currently live: {live_osds}")
            print(f"OSDs currently down: {list(down_osds)}")
            
            time.sleep(10)  # Adjust the sleep time as necessary
    
    except KeyboardInterrupt:
        print("Process interrupted by user. Exiting.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Thrash Ceph OSDs")
    parser.add_argument("daemon_type", choices=["classic", "crimson"], help="Type of OSD daemon to use (classic or crimson)")
    args = parser.parse_args()
    
    main(args.daemon_type)

