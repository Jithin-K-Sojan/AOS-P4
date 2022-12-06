# cs6210Project4
MapReduce Infrastructure

## Jithin Kallukalam Sojan, Kuilin Li

This project is an implementation of the map-reduce infrastructure meeting the specifications given in description.md and the map-reduce paper (https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf). We have verified our implementation succesffuly on both local setups and gradescope.

## Proto file

- MasterWorker: The service, with a single RPC, runJob().
- JobRequest: Sent by master to worker, with information necessary to execute map/reduce task.
- JobReply: Sent by the worker back to master when runJob returns, as the RPC return value.

## master.h

- The master is an asynchronous client that sends JobRequests to the specified workers filled with the necessary Map/Reduce task information. It keeps track of completed map/reduce tasks and their respective outputs.
- For worker failures or slowness, the master implements a "round-robin"-like scheduler. An index is maintained into the list of tasks to be done. Whenever the master determines that a worker needs a new task, it will increment the index, looping from the end of the list back to zero, until it points to a task that has yet to be done. The worker is then assigned that task, and runJob is asynchronously called.
- This does mean that multiple workers can be assigned to the same task, and this case is explicitly considered. When a worker completes, if it succeeded, the master will first check if the task is already done. If it isn't already done, it will write the result. Then, whether it succeeded or failed, another task is assigned to the worker.
- After all the map tasks are done, the master reorganizes input files and then does the reduce tasks in the same way.
- Since the master cannot cancel a task without stopping all communication with the worker, the master maintains an intermediate reducer output directory (in case a worker decides to create an output file after master cancels it, but before it receives the cancellation). At the end, the master copies all the correct output files to the real output.

### How we handle worker failure

When a worker fails, it will either signal failure through grpc, or return a failure in the status field of the JobReply. Either way, the master detects this failure and doesn't write the output into its task struct. The master schedules another task on the worker, and the task will likely be done by a different worker due to the scheduling algorithm.

### How we handle slow worker

When a worker is slow, its tasks will be picked up by other workers when the scheduler loops back around to that task. There is also a timeout of 100 seconds, if a worker does not respond by that time, it is assumed to be frozen and the master cancels the worker (and then schedules it another task). In the extreme case where there is only one working worker, and all the others are extremely slow, the working worker will still have the opportunity to do all of the tasks because a worker working on a task doesn't block anything - only a task being done skips it from the round-robin cycle.

## file_shard.h
- We use file streams to create shards of approximately map_kilobytes size (with EOL lining). Each file shard consists of miniShards that are used to split the inputFiles in accordance with descriprtion.md.

## worker.h

NOTE: The async server boiler plate code is based on the grpc helloworld implementation given here - https://github.com/grpc/grpc/blob/master/examples/cpp/helloworld/greeter_async_server.cc

- The worker provides the MasterWorker service, which takes as input JobRequest from the master containing details about the map/reduce job and returns JobResponse with information about intermediate/output files according to the task. The worker provides its service asynchronously.
- For each Map request, the worker parses the input shard and feeds each line to the BaseMapper's map function.
- The worker then creates a separate file for each parition, writes the key-value pairs and returns the intermediate file info to master.
- For each Reduce request, the worker parses each provided intermediate file (of a particular partition) and adds the key-value pairs to an ordered_map of keys to vector of values. It then feeds each key and corresponding value vector to the BaseReducers reduce function.
- The worker then creates an output file, writes the key-value pairs and returns to the master.

## mr_tasks.h

- The BaseMapperInternal, upon emit, takes the output key, hashes it, and stores it in the appropriates partition bucket, which will be later written to disk.
- The BaseReducerInternal, upon emit stores the key value pair which will be later written to disk.
