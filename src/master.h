#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"

#include <grpc++/grpc++.h>
#include <filesystem> 
#include "masterworker.grpc.pb.h"

using grpc::ClientAsyncResponseReader;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::CompletionQueue;
using grpc::ServerContext;

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
    bool run_ran = false; // just in case

    const MapReduceSpec _mr_spec;

    size_t _tasks_idx = 0; // for scheduler, which next one to do

    // map tasks, one per FileShard
    struct MapTask {
      const FileShard shard;

      bool done;
      std::string fn;
      masterworker::JobReply *reply;

      void fill_request(masterworker::JobRequest& req, const std::string& output_filename) {
        req.set_map_reduce(0);
        req.set_job_id("map_" + std::to_string((size_t)this));
        req.set_output_filename(output_filename);

        for (auto ms : shard.miniShards) {
          masterworker::FileArgs* fa = req.add_input_files();
          fa->set_file_path(ms.fileName);
          fa->set_start_offset(ms.start);
          fa->set_end_offset(ms.end);
        }
      };

      MapTask(const FileShard& s) : shard(s), done(false), reply(NULL) {};
    };
    std::vector<MapTask> _map_tasks;

    // reducer tasks, there are R of them
    struct ReduceTask {
      size_t partition;
      const std::vector<MapTask>& map_tasks;
      std::string output_dir;

      bool done;
      std::string fn;
      masterworker::JobReply *reply;

      void fill_request(masterworker::JobRequest& req, const std::string& output_filename) {
        req.set_map_reduce(1);
        req.set_job_id("reduce_" + std::to_string((size_t)this));
        req.set_output_filename(output_filename);

        for (auto &x : map_tasks) {
          assert(partition < x.reply->output_files_size());
          auto& f = x.reply->output_files(partition);

          masterworker::FileArgs* fa = req.add_input_files();
          fa->set_file_path(f.file_path());
          fa->set_start_offset(f.start_offset());
          fa->set_end_offset(f.end_offset());
        }
        req.set_output_dir(output_dir);
      };

      ReduceTask(const size_t& p, const std::vector<MapTask>& m, std::string o) : partition(p), map_tasks(m), output_dir(o), done(false), reply(NULL) {};
    };
    std::vector<ReduceTask> _reduce_tasks;

    // metadata for a single worker
    template<typename Task>
    class WorkerData {
      static_assert(std::is_same<Task, MapTask>::value || std::is_same<Task, ReduceTask>::value, "Task must be MapTask or ReduceTask");
      private:
        // current job's task slot
        // we only write to this if _task.done is false, and rpc succeeds
        Task *_task;

        const MapReduceSpec& _mr_spec;
        std::vector<Task>& _tasks;
        size_t& _tasks_idx;
        const std::string _ip;
        const std::string _output_dir;
        std::string _fn;
        bool _cancelled = false;

        CompletionQueue &_cq;
        std::shared_ptr<grpc::Channel> _ch;

        grpc::ClientContext *ctx = NULL;
        masterworker::JobReply *reply = NULL;
        grpc::Status status;

      public:
        WorkerData(const MapReduceSpec& mr_spec, std::vector<Task>& fsds, size_t &fsds_idx, std::string ip, CompletionQueue &cq, const std::string& output_dir) :
          _mr_spec(mr_spec), _tasks(fsds), _tasks_idx(fsds_idx), _ip(ip), _cq(cq), _output_dir(output_dir) { begin(); };

        void begin() {
          // determine which one to do
          // _tasks_idx is the next one, except if it's done already
          // it's okay to have all the workers working on the same last one, i think, at the way end
          // since they all can then just be cancelled when the first one completes
          {
            size_t initial_idx = _tasks_idx;
            while (_tasks[_tasks_idx].done) {
              _tasks_idx = (_tasks_idx+1)%_tasks.size();
              if (_tasks_idx == initial_idx) {
                // everything is done, nothing else to do (this drops out of the cq loop)
                return;
              }
            }
            assert(!_cancelled);
            _task = &_tasks[_tasks_idx];
            _fn = std::to_string(_tasks_idx) + "_" + std::to_string((size_t)this);
            printf("%s %s %s\n", _fn.c_str(), __func__, typeid(Task).name());
            _tasks_idx = (_tasks_idx+1)%_tasks.size();
          }

          // connect to server
          _ch = grpc::CreateChannel(_ip, grpc::InsecureChannelCredentials());
          auto stub = masterworker::MasterWorker::NewStub(_ch);

          // request job
          masterworker::JobRequest req;
          req.set_user_id(_mr_spec.user_id);
          req.set_num_partitions(_mr_spec.n_output_files);
          _task->fill_request(req, _fn);

          // dynamically allocate reply
          reply = new masterworker::JobReply();

          // set deadline and make context
          ctx = new grpc::ClientContext();
          ctx->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(1000));

          std::unique_ptr<ClientAsyncResponseReader<masterworker::JobReply>> rpc(
            stub->AsyncrunJob(ctx, req, &_cq));

          rpc->Finish(reply, &status, this);
        }

        void done() {
          printf("%s %s %s\n", _fn.c_str(), __func__, typeid(Task).name());

          // check task done or worker reported failure
          if (_task->done || reply->job_status() == 0) {
            fail();
            return;
          }

          // dispose of ctx and fn
          delete ctx;
          ctx = NULL;

          // set and pickup new task
          _task->reply = reply;
          _task->fn = _fn;
          _task->done = true;
          begin();
        }

        void fail() {
          printf("%s %s %s\n", _fn.c_str(), __func__, typeid(Task).name());
          delete reply;
          reply = NULL;
          delete ctx;
          ctx = NULL;
          // note: unreliable, worker may be yet to open file
          std::filesystem::remove(_output_dir + "/" + _fn);
          begin();
        }

        void cancel() {
          assert(!_cancelled);
          _cancelled = true;
          printf("%s %s %s\n", _fn.c_str(), __func__, typeid(Task).name());
          if (ctx) {
            ctx->TryCancel();
            // note: unreliable, worker may be yet to open file
            std::filesystem::remove(_output_dir + "/" + _fn);
          }
          // small memory leak here, but this is ok
        }
    };
};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) : _mr_spec(mr_spec) {
  _map_tasks.reserve(file_shards.size());
  for (auto s : file_shards) _map_tasks.emplace_back(s);
}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
  assert(!run_ran);
  run_ran = true;

  // single cq because master has single thread, we want to wait for it outside WorkerData
  CompletionQueue map_cq, reduce_cq;

  // connect to all workers for map task
  std::vector<WorkerData<MapTask>*> map_workers;
  map_workers.reserve(_mr_spec.worker_ipaddr_ports.size());
  for (std::string ip : _mr_spec.worker_ipaddr_ports) map_workers.push_back(new WorkerData<MapTask>(_mr_spec, _map_tasks, _tasks_idx, ip, map_cq, "unused"));

  void* tag;
  bool ok;

  // process workers until map task done
  while (!std::all_of(_map_tasks.begin(), _map_tasks.end(), [](auto &s){ return s.done; })) {
    if (!(map_cq.Next(&tag, &ok))) {
      // queue shutting down for some reason
      printf("queue shutdown?\n");
      return false;
    }
    if (!ok) ((WorkerData<MapTask>*)tag)->fail();
    else ((WorkerData<MapTask>*)tag)->done();
  }

  // make directory for output
  const std::string inter_reducer_output = "inter_reducer_output"; // dir name for intermediate master outputs
  std::filesystem::create_directory(inter_reducer_output);

  // cancel all workers for reduce (all but one will be busy on the last one)
  for (auto w : map_workers) w->cancel();

  // drain the queue just in case
  map_cq.Shutdown();
  while (map_cq.Next(&tag, &ok)) {
    if (!ok) ((WorkerData<MapTask>*)tag)->fail();
    else ((WorkerData<MapTask>*)tag)->done();
  }

  // set up reduce tasks
  _reduce_tasks.reserve(_mr_spec.n_output_files);
  for (size_t i=0; i<_mr_spec.n_output_files; i++) _reduce_tasks.emplace_back(i, _map_tasks, inter_reducer_output);

  // begin reduce task (note: we cannot destruct any workers since old callbacks might still be in cq... so just use a new cq)
  _tasks_idx = 0;
  std::vector<WorkerData<ReduceTask>*> reduce_workers;
  reduce_workers.reserve(_mr_spec.worker_ipaddr_ports.size());
  for (std::string ip : _mr_spec.worker_ipaddr_ports) reduce_workers.push_back(new WorkerData<ReduceTask>(_mr_spec, _reduce_tasks, _tasks_idx, ip, reduce_cq, inter_reducer_output));

  // process workers until reduce task done
  while (!std::all_of(_reduce_tasks.begin(), _reduce_tasks.end(), [](auto &s){ return s.done; })) {
    if (!(reduce_cq.Next(&tag, &ok))) {
      // queue shutting down for some reason
      printf("queue shutdown?\n");
      return false;
    }
    if (!ok) ((WorkerData<ReduceTask>*)tag)->fail();
    else ((WorkerData<ReduceTask>*)tag)->done();
  }

  // cancel all workers to delete 
  for (auto w : reduce_workers) w->cancel();

  // drain the queue just in case
  reduce_cq.Shutdown();
  while (reduce_cq.Next(&tag, &ok)) {
    if (!ok) ((WorkerData<ReduceTask>*)tag)->fail();
    else ((WorkerData<ReduceTask>*)tag)->done();
  }

  // copy correct outputs to real output dir
  std::filesystem::create_directory(_mr_spec.output_dir.c_str());
  for (auto &t : _reduce_tasks) {
    std::filesystem::copy((inter_reducer_output + "/" + t.fn).c_str(), (_mr_spec.output_dir + "/" + t.fn).c_str());
  }

	return true;
}