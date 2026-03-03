"""
Represents an abstract job which serially executes a command.
"""


from typing import Any, Union, List 
from job import SerializedJob, Job, JobState, job_deco, JobList, JSONType
from util import (
    run_command, 
    check_property, 
    file_contains,
    remove_substring,
    current_time_iso 
)


@job_deco("serial_command_job") 
class SerialCommandJob(Job):
    """
    A job which executes a command serially.

    Properties:
        cmd (Union[List[str], str]): the command to execute.
    """


    def clear_state():
        """
        Clears our internal state stored in the "started" and "ended" files.
        """

        # No per-job internal state exists.        
        pass


    def __init__(self, cmd: Union[List[str], str]):
        super().__init__() 
        self.cmd = cmd


    def __hash__(self) -> int:
        if isinstance(self.cmd, str):
            return hash(self.cmd)
        else:
            return hash(" ".join(c for c in self.cmd))


    def param_dict(self) -> dict[str, JSONType]: 
        return {
            "cmd": self.cmd
        }
    

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


    def recreate(self, job: SerializedJob) -> Job:
        super().recreate(job)
        
        check_property(job.params.keys(), "cmd", type(self))
        self.cmd = job.params["cmd"]
    
    
    def can_launch(self) -> None:
        """
        Assume no resource constraints.
        """
        return True


    def launch(self) -> None:
        try:
            self.start_time = current_time_iso()
            run_command(self.cmd)
            self.state = JobState.COMPLETED 
            self.end_time = current_time_iso()
        except Exception as ex:
            # In the case of an exception, the command failed to run. Print the
            # error, change our state to failed, and continue.
            self.state = JobState.FAILED
            print((
                f"Warning: we failed to run the command \"{self.cmd}\". We're "
                f"setting our state to FAILED and continuing. The error is "
                f"given below. "
            ))
            print(ex)
        

    def is_running(self) -> bool: 
        # All of our simple commands are serial, so we will never call this and
        # be running a command.
        return False 


    def exit(self) -> None:
        raise NotImplementedError() 


    def poll_state(self) -> JobState:
        """
        Our state is managed through the "launch" command.
        """
        return self.state
