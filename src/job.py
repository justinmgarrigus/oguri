"""
This defines the inferface for a job. A "job" represents a single, discrete task 
to perform. Jobs can optionally be dependent on other jobs and may only run when 
those dependencies are complete. Jobs may also only be launched when resource 
restrictions are met (i.e., on the number of jobs currently running, on the 
number of GPUs available or the amount of memory available, etc.). This is 
highly configurable by design. Jobs can also be resumed: in the case of long-
running jobs which corresponded to a prior monitoring session, job objects can 
be resumed.
"""


from abc import ABC, abstractmethod
from typing import Type, Callable, Any, Optional, Union, List, Dict
from enum import Enum
from dataclasses import dataclass, asdict
from datetime import datetime
import json
import os 
import re 
from util import assert_file_exists


JSONPrimitive = Union[str, int, float, bool, None]
JSONType = Union[JSONPrimitive, List["JSONType"], Dict[str, "JSONType"]]


class JobState(Enum):
    UNKNOWN = "UNKNOWN"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
   

@dataclass
class SerializedJob:
    cls_id: str 
    job_id: int
    params: Dict[str, JSONType]
    state: JobState
    start_time: Optional[str]
    end_time: Optional[str] 


class Job(ABC):
    """
    Represents a handler for a job. The job may be asynchronously running on the 
    system.

    Properties: 
        start_time (Optional[datetime]): the time the job was started. If the 
            job was not yet started, equals None.
        state (JobState): our execution state. 
        _cls_id (str): an identifier for our derived type.
    """

    
    @abstractmethod
    def __init__(self):
        self.start_time = None
        self.end_time = None 
        self.state = JobState.UNKNOWN

    
    @abstractmethod
    def __hash__(self) -> int: 
        """
        Returns a unique identifier for this job.
        """
        
        raise NotImplementedError()
    
    
    @abstractmethod
    def param_dict(self) -> Dict[str, JSONType]:
        """
        Returns the parameters needed to recreate our Job instance again via the 
        __init__ constructor in the future. This should ideally be equal to the 
        parameters initially passed to us when we were executed for the first 
        time.
        """
        
        return {}  # It doesn't take parameters to recreate a base Job

    
    def serialize(self) -> SerializedJob:
        """
        Serializes our state into a reduced, consistent form which can be 
        recreated later.
        """
        
        if not hasattr(self, "_cls_id") or self._cls_id == "base_job":
            raise ValueError(
                "Error: either we attempted to serialize an object of abstract "
                "type Job, or the derived job did not use \"job_deco\" to "
                "decorate the class definition. We may only serialize derived "
                "types which were registered with \"job_deco\"."
            )
        
        return SerializedJob(
            self._cls_id, 
            hash(self),
            params=self.param_dict(), 
            state=self.state, 
            start_time=self.start_time, 
            end_time=self.end_time
        )

    
    @abstractmethod
    def __eq__(self, other: Any) -> bool:
        raise NotImplementedError() 

    
    @abstractmethod
    def recreate(self, job: SerializedJob) -> "Job":
        """
        Recreates a job object from the serialized info of a previous run.
        """
        
        self.state = job.state 
        
        def parse_time(s):
            if s is None:
                return None

            try:
                return datetime.fromisoformat(job.start_time)
            except ValueError as ex: 
                raise ValueError((
                    f"Error: a job object was attemped to be parsed with the value "
                    f"\"{job.start_time}\", though this is not in ISO format."
                )) from ex 
        
        self.start_time = parse_time(job.start_time)
        self.end_time = parse_time(job.end_time) 

    
    @abstractmethod
    def can_launch(self) -> bool:
        """
        Returns True if a job can launch, or False otherwise.
        """
        
        raise NotImplementedError()


    @abstractmethod
    def launch(self) -> None:
        """
        Runs a job. This job may or may not run asynchronously. If this job 
        could not be launched, throws an error.
        """
        
        raise NotImplementedError() 

    
    @abstractmethod
    def is_running(self) -> bool: 
        """
        Returns True if this job is currently running, or False otherwise. 
        """
        
        raise NotImplementedError() 

    
    @abstractmethod
    def exit(self) -> None:
        """
        Exits a job. If this job is not currently running, then does nothing.
        Stalls until the job is actually exited; if this job cannot be exited, 
        then raises an error. 
        """
        
        raise NotImplementedError()


    @abstractmethod
    def poll_state(self) -> JobState:
        """
        Polls for the state of the object. Also updates our internal "state"
        property.
        """

        raise NotImplementedError()


    @abstractmethod
    def clear_state(self) -> None:
        """
        Clears our internal state, if we maintain any kind of per-job internal
        state.
        """

        raise NotImplementedError() 


"""
Maps string identifiers to types, so jobs can be dynamically reconstructed in
other sessions than when they originally were created.
"""
_JOB_REGISTRY: Dict[str, Type["Job"]] = {}


def job_deco(name: str) -> Callable[[Job], Job]:
    """
    Decorator, registers a Job subclass in our job registry which allows 
    recreating Job objects later.
    """
    def deco(cls: Job) -> Job:
        assert re.match(r"[a-zA-Z0-9_]", name) is not None, (
            f"Error: class identifier \"{name}\" can only contain alphanumeric "
            "characters."
        )
        cls._cls_id = name
        _JOB_REGISTRY[name] = cls
        return cls
    return deco


class JobList:
    """
    The JobList is a register of all jobs associated with some goal. It is
    maintained in a physical file on the system. This file is updated in real-
    time as updates are made to each job.

    Properties: 
        _jobs (list[Job]): the list of jobs, regardless of whether they're
    """


    def __init__(self, fname: str, reset: bool = False):
        """
        Creates a new file if it doesn't already exist, or attaches us to the 
        existing file if it exists. If "reset" is True, then always deletes this
        file if it exists.
        """

        self.fname = fname
        if reset and os.path.exists(self.fname):
                os.remove(self.fname)
        
        if os.path.exists(fname):
            self._jobs = self._read()
        else:
            self._jobs = []


    def __iter__(self):
        return (job for job in self._jobs) 


    def _read(self) -> List[Job]:  
        """
        Reads the contents of the job list file and sets our internal state to
        match it. The job list is a json file containing a list of objects, with
        each object having the following keys:
            cls_id (str): the identifier for the registered Job type.
            job_id (int): the identifier for this particular Job instance.
            params (dict[str, JSONType]): a set of parameters for the job.
            state (JobState): the state of this job.
            start_time (Optional[str]): if the job was not started, equals None.
                Otherwise, equals the formatted time of the job being started.
            end_time (Optional[str]): if the job was not ended, equals None.
                Otherwise, equals the formatted time of the job ending.
        """
        assert_file_exists(self.fname) 
        with open(self.fname, "r") as f:
            try:
                content = json.load(f)
                content_list = [] 
                for item in content:
                    cls_id = item["cls_id"]
                    job_id = item["job_id"]
                    params = item["params"]
                    state = JobState(item["state"])  
                    start_time = item["start_time"]
                    end_time = item["end_time"] 
                    content_list.append(
                        SerializedJob(
                            cls_id=cls_id, 
                            job_id=job_id, 
                            params=params, 
                            state=state,  
                            start_time=start_time, 
                            end_time=end_time
                        )
                    )

            except Exception as ex:
                raise ValueError(
                    f"Error: \"{self.fname}\" cannot be parsed as JSON, or it "
                    f"does not match the required format."
                ) from ex
        
        # Now we have a list of SerializedJson objects. We want to obtain actual
        # Job objects, so recreate them.
        jobs = []
        for job in content_list:
            if job.cls_id not in _JOB_REGISTRY.keys():
                raise ValueError(
                    f"Error: the job list at \"{self.fname}\" contains a job "
                    f"which corresponds to a class we don't know. The classes "
                    f"we know have keys {_JOB_REGISTRY.keys()}."
                )

            cls = _JOB_REGISTRY[job.cls_id]
            jobs.append(cls(**job.params)) 
        
        return jobs 

    
    def register_job(self, job: Job) -> bool: 
        """
        Adds a job to our registry. Returns True if the job was not already 
        present in our registry and if it was pushed to the file, or False if it 
        did already exist. The file is not immediately updated. 
        """
        
        if job in self._jobs: 
            return False
        self._jobs.append(job)
        return True

    
    def update_state(self, job: Job, state: JobState) -> None:
        """
        Updates the state of a job. If the job doesn't exist in our registry, 
        then raises an error.
        """
        
        serialized_job = self._get(job)
        serialized_job.state = state


    def flush(self) -> None: 
        """
        Writes our job states to the file.
        """
        
        # Serialize our jobs and convert them to dictionaries.
        def enum_default(o):
            if isinstance(o, Enum): 
                return o.value 
            raise TypeError(
                f"Error: Object of type {type(o)} is not JSON serializable."
            )
        
        content = [
            asdict(job.serialize())
            for job in self._jobs
        ]
        
        with open(self.fname, "w") as f:
            json.dump(content, f, default=enum_default, indent=2, 
                separators=(",", ":"))


    def poll_states(self) -> None:
        """
        Polls for the states of each Job (updating their "state" properties in 
        the process) and updates/flushes our registry at the end.
        """

        for job in self._jobs:
            job.poll_state() 
        self.flush() 
