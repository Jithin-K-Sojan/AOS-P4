# cs6210Project4
MapReduce Infrastructure

## Jithin Kallukalam Sojan, Kuilin Li

This project is an implementation of the map-reduce infrastructure meeting the specifications given in description.md and the map-reduce paper (https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf). We have verified our implementation succesffuly on both local setups and gradescope.

## Proto file

- MasterWorker: The service, with a single RPC, runJob().
- JobRequest: Sent by master to worker, with information necessary to execute map/reduce task.
- JobReply:

## master.h

- The master is an asynchronous client that sends JobRequests filled with the necessary Map/Reduce task information. It keeps track of completed map/reduce tasks and their respective outputs.

### How we handle worker failure

### How we handle slow worker

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




