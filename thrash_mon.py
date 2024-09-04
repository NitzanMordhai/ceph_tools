import os
import random
import time
import subprocess
import argparse
import signal
import json

def get_live_mons():
    mon_status = subprocess.check_output(["ceph", "mon", "stat", "-f", "json"]).decode("utf-8")
    mon_status_json = json.loads(mon_status)
    
    live_mons = [mon['name'] for mon in mon_status_json['quorum']]
    print(f"mon status: {mon_status_json} live mons: {live_mons}")
    return live_mons

def kill_mon(mon_name):
    pid_file = f"/home/nmordech/ceph/build/out/mon.{mon_name}.pid"
    if not os.path.isfile(pid_file):
        print(f"PID file for monitor {mon_name} not found.")
        return
    with open(pid_file, 'r') as f:
        pid = int(f.read().strip())
    os.kill(pid, signal.SIGTERM)
    print(f"Killed monitor {mon_name} (PID: {pid}).")
    time.sleep(10)

def wait_until_leader_changed(current_leader):
    leader = current_leader
    while leader == current_leader:
        mon_status = subprocess.check_output(["ceph", "mon", "stat", "-f", "json"]).decode("utf-8")
        mon_status_json = json.loads(mon_status)
        leader = mon_status_json['leader']
        if leader == current_leader:
            print(f"Leader still {leader}.")
            time.sleep(1)
    print(f"Leader changed from {current_leader} to {leader}.")
    return leader

def revive_mon(mon_name):
    mon_command = f"ceph-mon -i {mon_name} -c /home/nmordech/ceph/build/ceph.conf &"
    subprocess.run(mon_command, shell=True)
    print(f"Revived monitor {mon_name}.")

def change_quorum(old_quorum):
    mon_stat = subprocess.check_output(["ceph", "mon", "stat", "-f", "json"]).decode("utf-8")
    mon_stat_json = json.loads(mon_stat)
    
    quorum = mon_stat_json['quorum']
    print(f"Current quorum: {quorum} old quorum: {old_quorum}") 
    if old_quorum and old_quorum != quorum:
        print("Quorum unchanged.")
        return quorum
    
    print("Quorum didn't changed.")
    return quorum

def main():
    live_mons = get_live_mons()
    down_mons = set()
    old_quorum = None
    print(f"Initial live monitors: {live_mons}")
    mon_to_kill = live_mons[0]
    try:
        while True:
            live_mons = get_live_mons()
            if not live_mons:
                print("No live monitors to kill. Exiting.")
                break

            
            kill_mon(mon_to_kill)
            new_leader = wait_until_leader_changed(mon_to_kill)
            #sleep for 5 minutes
            time.sleep(300)
            revive_mon(mon_to_kill)
            print(f"Revived monitor {mon_to_kill} append.")
            old_quorum = change_quorum(old_quorum)
            time.sleep(300)
            mon_to_kill = new_leader
    
    except KeyboardInterrupt:
        print("Process interrupted by user. Exiting.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Thrash Ceph monitors and change quorum")
    args = parser.parse_args()
    
    main()
