from job import JobList
from command_job import SerialCommandJob
import argparse
import os 


def get_args():
    parser = argparse.ArgumentParser(
        prog="test.py", 
        description="simple test for the script working"
    )
    parser.add_argument("--reset", help="resets the state of the system", 
        action="store_true")
    return parser.parse_args()


args = get_args() 
if args.reset:
    if os.path.exists("foo.txt"):
        os.remove("foo.txt") 
    if os.path.exists("bar.txt"):
        os.remove("bar.txt")


jobs = [
    SerialCommandJob("asfdsjkfasdfhjasdfha bar.txt"), 
    SerialCommandJob("touch foo.txt"), 
    SerialCommandJob("touch bar.txt")
]

job_list = JobList("jobs.json", reset=args.reset)
for job in jobs:
    job_list.register_job(job)
job_list.flush()

# Execute each job.
for job in job_list:
    if job.can_launch():
        job.launch()

job_list.poll_states() 
