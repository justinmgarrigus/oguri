from oguri.job import JobList
from oguri.async_job import AsyncCommandJob
import argparse
import os
import time 


def get_args(): 
    parser = argparse.ArgumentParser(
        prog="test.py", 
        description="simple test for the script working"
    )
    parser.add_argument("--reset", help="resets the state of the system", 
        action="store_true")
    return parser.parse_args()


args = get_args()


jobs = [
    AsyncCommandJob("bash -c 'sleep 30 && touch done'"),
    AsyncCommandJob("touch sophie") 
]

job_list = JobList("jobs.json", reset=args.reset) 
for job in jobs:
    job_list.register_job(job) 
job_list.flush() 

# Execute each job once.
for job in job_list.launchable_jobs():
    job.launch() 

# Keep program open and print while jobs are not finished.
while len(job_list.running_jobs()) > 0:
    print(job_list)
    print()
    time.sleep(10)
    job_list.poll_states()

print("Final state:")
print(job_list)
