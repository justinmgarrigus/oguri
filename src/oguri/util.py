import subprocess 
import os
from typing import Set, Union, List, Optional, Any
from datetime import datetime


class CommandFailedError(Exception): 
    def __init__(self, message: str):
        super().__init__(message) 


def assert_file_exists(fname) -> None:
    assert os.path.exists(fname), f"Error: {fname} does not exist." 


def file_contains(
    fname: str, 
    substr: str, 
    error_if_missing: bool = False
) -> bool:
    """
    Returns True if the file contains the given substring, or False otherwise. 
    If the file does not exist, then returns False.
    """
    
    if os.path.exists(fname):
        with open(fname, "r") as f:
            return substr in f.read()
    elif exit_if_missing:
        raise FileNotFoundError(f"Error: \"{fname}\" does not exist.") 
    
    return False


def remove_substring(fname: str, substr: str) -> None: 
    """
    Removes all occurrences of a given substring from the file. If the file 
    doesn't exist, then doesn't do anything. 
    """
    
    if os.path.exists(fname): 
        with open(fname, "r") as f:
            text = f.read() 
        text.replace(substr, "")
        with open(fname, "w") as f:
            f.write(text) 


def run_command(
    cmd: Union[str, List[str]], 
    allow_nonzero_code: bool = False
) -> str: 
    """
    Utility method for "subprocess.run". Runs a command, exits if it returns a 
    nonzero error code, and returns the concatenated stdout and stderr. If 
    "allow_nonzero_code" is True, then nonzero exit codes are ignored. If the 
    given command is a string, runs this with "shell=True".
    """
    
    assert all(isinstance(c, str) for c in cmd), (
        f"Error: the command must only contain strings.\n  "
        + "\n  ".join(f"{idx}.) \"{c}\" [{type(c)}]" for c in cmd)
    )

    proc = subprocess.run(
        cmd, 
        capture_output=True, 
        text=True, 
        shell=isinstance(cmd, str)
    )
    if proc.returncode != 0 and not allow_nonzero_code:
        raise CommandFailedError(
            f"Error: command \"{cmd}\" returned a non-zero exit code "
            f"{proc.returncode}.\n{proc.stdout}\n{proc.stderr}"
        )

    return proc.stdout + proc.stderr 


def check_property(params: dict[str, "Job"], key: str, cls: type) -> Any:
    """
    Checks that the "params" contain the given "key". If it does, return that 
    key's value. If it doesn't, raises a detailed error. This should be used 
    when attemping to cast a SerializedJob to a derived Job object. 
    """

    try:
        return params[key]
    except:
        raise ValueError((
            f"Error: attempting to recreate a {cls} instance from a "
            f"SerializedJob instance which has no \"{key}\" property. Is it "
            f"really of type {cls}?"
        ))


def time_iso(time_obj: Optional[datetime]) -> str:
    """
    Returns the datetime object as an ISO format string. If the object is None,
    then returns the current time in ISO format.
    """
    
    if time_obj is None:
        time_obj = datetime.now().astimezone() 
    return time_obj.isoformat(timespec="seconds")


def time_diff(
    start_time: Union[str, datetime], 
    end_time: Optional[Union[str, datetime]] = None
) -> float:
    """
    Returns the time in seconds between "start_time" and "end_time". If 
    "end_time" is None, this equals the current time. If "start_time" is a str, 
    then it must be in the format returned by "current_time_iso()". 
    """
    
    if isinstance(start_time, str):
        start_time = datetime.fromisoformat(start_time) 
    if end_time is None: 
        end_time = datetime.now().astimezone() 
    elif isinstance(end_time, str): 
        end_time = datetime.fromisoformat(end_time) 
    
    return (end_time - start_time).seconds
