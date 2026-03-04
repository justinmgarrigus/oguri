"""
Represents an abstract job which serially executes a command.
"""


from datetime import datetime
from typing import Any, Union, List, Dict 
import time 
from oguri.job import SerializedJob, Job, JobState, job_deco, JSONType
from oguri.util import (
    CommandFailedError, 
    run_command, 
    check_property, 
    file_contains,
    remove_substring,
    time_iso,
    time_diff
)


@job_deco("serial_command_job") 
class SerialCommandJob(Job):
    """
    A job which executes a command serially.

    Properties:
        cmd (Union[List[str], str]): the command to execute.
        launch_attempts (int): the amount of times we can launch this command.
            As soon as the command succeeds, we don't try it again.
        retry_delay (float): the amount of time in seconds we should wait before 
            retrying a command.
    """


    def __init__(
        self, 
        cmd: Union[List[str], str], 
        launch_attempts: int = 1, 
        retry_delay: float = 0.0
    ):
        super().__init__()
        self.cmd = cmd
        self.state = JobState.PENDING
        self.launch_attempts = launch_attempts 
        self.retry_delay = retry_delay


    def __hash__(self) -> int:
        if isinstance(self.cmd, str):
            return hash(self.cmd)
        else:
            return hash(" ".join(c for c in self.cmd))


    def param_dict(self) -> Dict[str, JSONType]: 
        return {
            "cmd": self.cmd, 
            "launch_attempts": self.launch_attempts, 
            "retry_delay": self.retry_delay
        }

    
    def __str__(self) -> str: 
        state = (
            f"[{self.state.value}" + (
                f", {self.launch_attempts} attempts remaining" 
                if self.state == JobState.FAILED else ""
            ) + 
            "]"
        )
        cmd_str = (
            self.cmd if isinstance(self.cmd, str)
            else " ".join(self.cmd)
        )
        return f"{state}: {cmd_str}" 


    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, SerialCommandJob): 
            return False
        
        if isinstance(self.cmd, str):
            return self.cmd == other.cmd
        else:
            return (
                len(self.cmd) == len(other.cmd) and
                all(c1 == c2 for c1, c2 in zip(self.cmd, other.cmd))
            )


    def restore(self, job: SerializedJob) -> SerialCommandJob:
        super().restore(job) 
        self.cmd = check_property(job.params, "cmd", type(self)) 
        self.launch_attempts = check_property(job.params, "launch_attempts", 
            type(self)) 
        self.retry_delay = check_property(job.params, "retry_delay", type(self)) 

    
    def can_launch(self) -> None:
        """
        Assume no resource constraints.
        """
        return self.launch_attempts > 0 


    def launch(self) -> None:
        # We can only launch if enough time has passed. Our "start_time" says 
        # the time that we launched ourselves last, if it exists.
        if self.start_time:
            seconds = time_diff(self.start_time)
            time.sleep(max(0, self.retry_delay - seconds))
        
        try:
            self.start_time = datetime.now().astimezone()
            run_command(self.cmd)
            self.state = JobState.COMPLETED 
            self.end_time = datetime.now().astimezone()
        except CommandFailedError as ex:
            # In the case of an exception, the command failed to run. Print the
            # error, change our state to failed, and continue.
            self.state = JobState.FAILED
            print((
                f"Warning: we failed to run the command \"{self.cmd}\". We're "
                f"setting our state to FAILED and continuing. The error is "
                f"given below. "
            ))
            print(ex)
        
        self.launch_attempts -= 1 
        

    def poll_state(self) -> JobState:
        """
        Our state is managed through the "launch" command.
        """
        return self.state
