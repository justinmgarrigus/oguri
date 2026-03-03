# oguri

`oguri` is a tool for scheduling jobs on computers. A "job" is any kind of computing task: jobs may have dependencies on other jobs, may require certain resources like GPUs or memory, and may be large in number and duration. Jobs may be scheduled across multiple sessions, may be resumed after a long idle time, and may be quit and restarted. This tool makes it easy to start, monitor, and obtain results from these jobs with the least amount of friction and most reusability across different tasks.

This project was motivated by a recurring need for task-specific job schedulers. Many of my tasks require specific resources, like GPUs and memory, and have dependencies between them in a branching way. For example, one job may trace a GPU application while another will analyze those traces: instead of running the analysis while a GPU is still allocated, we need a way to add a dependency between these two. Furthermore, if a job unexpectedly quits, then we must have another job fill its space. This script combines many of my needs into a one-size-fits-all solution. 

The project is designed around extensible classes: the abstract `Job` structure defines the actions that sub-classes must fulfill, including in launching commands, updating internal state, checking status, and signaling if it can begin. Derived classes can implement these tasks to be more specialized. For instance, the `SerialCommandJob` simply runs a command without resource requirements. The `ParallelCommandJob` has fancier utilities to monitor state while a command is running in the background, and this can be further specialized to account for specific tool executions (e.g., simulators, which are famously very long-running).

`oguri` is developed in Python, because I am the most comfortable in developing in that quickly. Job states/statuses are written in JSON for the sake of readability. 

This project shares the name of the main character of Uma Musume: Cinderella Gray.
