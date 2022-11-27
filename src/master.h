#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"

#include <grpc++/grpc++.h>
#include "masterworker.grpc.pb.h"

using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::CompletionQueue;
using grpc::ServerContext;
using grpc::Status;

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/

    // state machine for a single worker - manages own malloc, must be new'd
    class WorkerData {
      private:
        const MapReduceSpec& _mr_spec;
        const std::vector<FileShard>& _file_shards;

        enum State { START, CREATE_MAP, WAIT_MAP, IDLE_MAP, CREATE_REDUCE, WAIT_REDUCE, DONE };
        State _status;

      public:
        WorkerData(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards, string ip) :
          _mr_spec(mr_spec), _file_shards(_file_shards), _ip(ip), _status(START) { this->step(); };

        void step() {
          if (_status == START) {
            // connect to server
            new WorkerData(mr_spec, file_shards, ip);

            masterworker::MasterWorker::AsyncService service;
            CompletionQueue cq;

            auto ch = grpc::CreateChannel(ip, grpc::InsecureChannelCredentials());
            auto stub = masterworker::MasterWorker::NewStub(ch);
            // wip
          }
        }

        void fail() {
          // wip
        }
    };
};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {
  assert(mr_spec.n_workers == mr_spec.worker_ipaddr_ports.size());

  // single cq because master has single thread (todo check if this is okay)
  masterworker::MasterWorker::AsyncService service;
  CompletionQueue cq;

  // connect to workers
  for (string ip : mr_spec.worker_ipaddr_ports) new WorkerData(mr_spec, file_shards, ip);

  // register fail handler
  // wip AsyncNotifyWhenDone to fail afer casting tag

  // receive loop
  void* tag;
  bool ok;
  while (1) {
    assert(cq->Next(&tag, &ok));
    assert(ok);
    ((WorkerData*)tag).step();
  }

  // wip ===
}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	return true;
}