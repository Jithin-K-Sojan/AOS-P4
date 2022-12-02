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
    const MapReduceSpec _mr_spec;

    struct FileShardData {
      const struct FileShard shard;
      bool done = false;

      ClientContext ctx;
      masterworker::JobReply reply;
      Status status;

      FileShardData(const FileShard &s) : shard(s) {}
    };
    std::vector<struct FileShardData> _fs;
    size_t _fs_idx = 0;

    // state machine for metadata for a single worker, must be malloc'd/new'd
    // passes own tag into grpc callback for casting later back to WorkerData*
    // not reused for reduces
    class WorkerData {
      private:
        const MapReduceSpec& _mr_spec;
        std::vector<FileShardData>& _fs;
        size_t *_fs_idx_ptr;
        const std::string _ip;
        const bool _map_reduce; // whether it's map or reduce

        CompletionQueue &_cq;
        std::shared_ptr<grpc::Channel> _ch;

      public:
        WorkerData(const MapReduceSpec& mr_spec, const std::vector<FileShardData>& fs, size_t *fs_idx_ptr, std::string ip, bool map_reduce, CompletionQueue &cq) :
          _mr_spec(mr_spec), _fs(fs), _ip(ip), _fs_idx_ptr(fs_idx_ptr), _map_reduce(map_reduce), _cq(cq) { };

        void begin() {
          // get next one to schedule
          // *_fs_idx_ptr is the next one, except if it's done already
          // it's okay to have all the workers working on the same last one, i think, at the way end
          // since they all can then just be cancelled when the first one completes
          {
            size_t initial = *_fs_idx_ptr;
            while (_fs[*_fs_idx_ptr].done) {
              *_fs_idx_ptr = ((*_fs_idx_ptr)+1)%_fs.size();
              if ((*_fs_idx_ptr) == initial) {
                // should not loop all the way around, since then begin() should never have been called
                assert(false);
              }
            }
          }

          // connect to server
          _ch = grpc::CreateChannel(_ip, grpc::InsecureChannelCredentials());
          auto stub = masterworker::MasterWorker::NewStub(_ch);

          // request job
          masterworker::JobRequest req;
          req.set_job_id("job");
          req.set_user_id(_mr_spec.user_id);
          req.set_map_reduce(_map_reduce);
          req.set_num_partitions(_mr_spec.n_output_files);
          for (auto ms : _fs[*_fs_idx_ptr].shard.miniShards) {
            masterworker::FileArgs* fa = req.add_input_files();
            fa->set_file_path(ms.fileName);
            fa->set_start_offset(ms.start);
            fa->set_end_offset(ms.end);
          }
          req.set_output_dir("output_dir");

          std::unique_ptr<ClientAsyncResponseReader<masterworker::JobReply>> rpc(
            stub->AsyncrunJob(&_fs[*_fs_idx_ptr].ctx, req, _cq));

          rpc->Finish(&_fs[*_fs_idx_ptr].reply, &_fs[*_fs_idx_ptr].status, this);
        }

        void fail() {
          // wip
        }
    };
};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) : _mr_spec(mr_spec) {
  _fs.reserve(file_shards.size());
  for (auto s : file_shards) _fs.emplace_back(s);
}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
  // single cq because master has single thread, we want to wait for it outside WorkerData
  CompletionQueue cq;

  // connect to all workers
  for (std::string ip : _mr_spec.worker_ipaddr_ports) new WorkerData(_mr_spec, _fs, &_fs_idx, ip, cq);

  // register fail handler
  // wip AsyncNotifyWhenDone to fail afer casting tag

  // receive loop
  void* tag;
  bool ok;
  while (!std::all_of(_fs.begin(), _fs.end(), [](auto &s){ return s.state == FileShardData::FileShardState::DONE; })) {
    if (!(cq.Next(&tag, &ok))) {
      // queue shutting down
      return false;
    }
    if (!ok) ((WorkerData*)tag)->fail();
    else ((WorkerData*)tag)->step();
  }

	return true;
}