import subprocess 
import os
from typing import Set, Union, List
from datetime import datetime


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


def run_command(cmd: Union[str, List[str]]) -> str: 
    """
    Utility method for "subprocess.run". Runs a command, exits if it returns a 
    nonzero error code, and returns the concatenated stdout and stderr. If the 
    given command is a string, runs this with "shell=True".
    """
    
    proc = subprocess.run(
        cmd, 
        capture_output=True, 
        text=True, 
        shell=isinstance(cmd, str)
    ) 
    assert proc.returncode == 0, (
        f"Error: command \"{cmd}\" returned a non-zero exit code "
        f"{proc.returncode}.\n{proc.stdout}\n{proc.stderr}"
    )

    return proc.stdout + proc.stderr 


def check_property(keys: Set[str], key: str, cls: type) -> None:
    """
    Checks that the "keys" contain the given "key". If it doesn't, returns a
    detailed error. This should be used when attemping to cast a SerializedJob
    to a derived Job object. 
    """

    if key not in keys:
        raise ValueError((
            f"Error: attempting to recreate a {cls} instance from a "
            f"SerializedJob instance which has no \"{key}\" property. Is it "
            f"really of type {cls}?"
        ))


def current_time_iso() -> str:
    """
    Returns the current time in ISO format.
    """
    return datetime.now().astimezone().isoformat(timespec="seconds")
