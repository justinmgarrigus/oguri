"""
Represents a job which may complete asynchronously. This requires we have a 
mechanism for checking when the job actually completed. This should be extended
by something which has a particular task and method for checking if something 
completed. All asynchronous jobs run in Screen sessions and have a single 
command control them.
"""


from abc import ABC, abstractmethod
from typing import Dict, Union, List, Any
from datetime import datetime 
import shlex 
import re 
from oguri.job import SerializedJob, Job, JobState, job_deco, JSONType
from oguri.util import (
    run_command, 
    CommandFailedError, 
    check_property, 
    time_diff
)


class Screen: 
    """
    Class for managing Screen sessions. Screen sessions are used to run commands
    in the background. Each Screen session is associated with a single command.
    When the command is finished, the Screen session will exit.

    Properties:
        identifier (int): a unique identifier for this session.
    """


    def __init__(self, identifier: int):
        self.identifier = identifier


    def is_running(self):
        """
        Returns True if the Screen session is currently running, or False 
        otherwise.
        """
        
        # Each line in the screen output has this format.
        screen_ls = run_command(["screen", "-ls"], allow_nonzero_code=True) 

        reg = re.compile(r"\s*\d+\.(-?\d+)\s*.*")
        for line in screen_ls.split("\n"):
            if mat := reg.match(line):
                identifier = mat[1] 
                if str(self.identifier) == identifier:
                    return True
        return False


    def launch(self, cmd: Union[str, List[str]]):
        """
        Launches a command on a new Screen session. The Screen session must not
        be already running.
        """
        
        assert not self.is_running()
        

        full_cmd = ["screen", "-S", str(self.identifier), "-dm"]
        if isinstance(cmd, list):
            full_cmd += cmd
        else:
            full_cmd += shlex.split(cmd)

        run_command(full_cmd)

        # assert self.is_running()  # TODO: we need another method for checking
        ### if a screen session finished in order for us to know if it completed
        ### or failed. Also check that here.  


    def kill(self):
        """
        Kills this Screen session. If this Screen session is not currently
        running, does nothing.
        """
        
        try:
            run_command(["screen", "-S", str(self.identifier), "-X", "quit"])
        except CommandFailedError:
            # If the command failed, then that probably indicates the Screen
            # session never existed in the first place.
            pass

        assert not self.is_running()
    

@job_deco("async_command_job") 
class AsyncCommandJob(Job, ABC): 
    """
    A job which may complete asynchronously. Must be extended to suit a 
    particular use-case, including defining how we know the job has actually
    completed.

    Properties:
        cmd (Union[List[str], str]): the command to execute in the Screen 
            session. 
        screen (Screen): a reference to the Screen session. This only creates
            the Screen session once the job is actually launched.
    """


    def __init__(self, cmd: Union[List[str], str]) -> None:
        super().__init__() 
        self.cmd = cmd
        self.state = JobState.PENDING
        self.screen = Screen(hash(self)) 


    def __hash__(self) -> int:
        if isinstance(self.cmd, str):
            return hash(self.cmd) 
        else:
            return hash(" ".join(c for c in self.cmd)) 


    def param_dict(self) -> Dict[str, JSONType]:
        return { "cmd": self.cmd }


    def __str__(self) -> Dict:
        if self.state == JobState.RUNNING:
            seconds = time_diff(self.start_time) 
            minutes = seconds // 60 
            seconds %= 60
            state_str = f"[{self.state.value}, {minutes}m{seconds}s]"
        else:
            state_str = f"[{self.state.value}]" 

        cmd_str = (
            self.cmd if isinstance(self.cmd, str)
            else " ".join(self.cmd)
        )

        return f"{state_str}: {cmd_str}" 


    def __eq__(self, other) -> bool:
        if not isinstance(other, AsyncCommandJob): 
            return False
        
        if isinstance(self.cmd, str):
            return self.cmd == other.cmd
        else:
            return (
                len(self.cmd) == len(other.cmd) and
                all(c1 == c2 for c1, c2 in zip(self.cmd, other.cmd))
            )


    def restore(self, job: SerializedJob) -> "AsyncCommandJob": 
        super().restore(job)
        self.cmd = check_property(job.params, "cmd", type(self)) 
        self.screen = Screen(hash(self)) 


    def can_launch(self) -> None:
        """
        Assume no resource constraints.
        """
        return not self.screen.is_running()


    def launch(self) -> None:
        """
        Launches the command in a new Screen session.
        """
        
        # Create the Screen command.
        try:
            self.start_time = datetime.now().astimezone()
            self.screen.launch(self.cmd) 
            self.state = JobState.RUNNING
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


    def poll_state(self) -> JobState: 
        if self.state == JobState.RUNNING: 
            if self.screen.is_running():
                self.state = JobState.RUNNING 
            else:
                self.state = JobState.COMPLETED 
        return self.state 


    def clean(self) -> None:
        """
        Called once the user wants to completely free any resources associated
        with the job, including Screen sessions and other artifacts 
        automatically generated. Extend this to clean more resources.
        """
        
        # We don't know of any resources we need cleaning.
        pass 


    def exit(self) -> None:
        """
        Exits a job. If this job is not currently running, then does nothing.
        Stalls until the job is actually exited; if this job cannot be exited, 
        then raises an error. 
        """
        
        self.screen.kill()
