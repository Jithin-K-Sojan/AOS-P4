#pragma once

#include <mr_task_factory.h>
#include "mr_tasks.h"

// ADDED
#include <fstream>
#include <filesystem>
#include <vector>
#include <map>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>

#include "masterworker.grpc.pb.h"

// ADDED
using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;

using masterworker::MasterWorker;
using masterworker::JobRequest;
using masterworker::JobReply;
using masterworker::FileArgs;

// Async server boiler-plate code based on - https://github.com/grpc/grpc/blob/master/examples/cpp/helloworld/greeter_async_server.cc

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/

		class CallData;

		void HandleRpcs();
		
		// Server Completion queue.
		std::unique_ptr<ServerCompletionQueue> cq_;
		// Store service representation.
		MasterWorker::AsyncService service_;
		std::unique_ptr<Server> server_;

		std::string ip_addr_port_;

};

/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) : ip_addr_port_(ip_addr_port) {
	
}

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

class Worker::CallData {
	public:

	CallData(MasterWorker::AsyncService* service, ServerCompletionQueue* cq)
		: service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
		Proceed();
	}

	void runMapTask() {
		auto mapper = get_mapper_from_task_factory(request_.user_id());
		
		int num_partitions = request_.num_partitions();

		mapper->impl_->setNumOfPartitions(num_partitions);

		for (FileArgs fileArg_: request_.input_files()) {
			std::ifstream inputFileStream(fileArg_.file_path(), std::ios::in);
			std::string inputString;

			inputFileStream.seekg(fileArg_.start_offset());

			while (inputFileStream.tellg()!=fileArg_.end_offset()) {

				if (getline(inputFileStream, inputString)){

					mapper->map(inputString);

					if (inputFileStream.tellg()==-1){
						// EOF
						break;
					}
				}
			}

			inputFileStream.close();
		}

		// Creating intermediate folder
		std::string intermediateName = "inter_" + request_.job_id() + "_" + std::to_string(getpid());
		if (mkdir(intermediateName.c_str(),0777)){
			std::cout<<"Error in creating directory "<<intermediateName<<std::endl;
		}

		// Writing to intermediate files.
		for (int i = 0; i<num_partitions; i++){
			std::string interFilePath = intermediateName + "/" + std::to_string(i) + ".txt";
			std::string interFileAbsPath = std::filesystem::current_path() / std::filesystem::path(interFilePath);

			std::ofstream outputFileStream(interFilePath, std::ios::out);

			for (std::pair<std::string,std::string> p : mapper->impl_->mapResult[i]){
				// Putting the key and value in consequent lines incase the strings could have spaces themselves.
				outputFileStream << p.first << std::endl;
				outputFileStream << p.second <<std::endl;
			}

			FileArgs* newFileArgs = reply_.add_output_files();
			newFileArgs->set_file_path(interFileAbsPath);
			newFileArgs->set_start_offset(0);
			newFileArgs->set_end_offset(outputFileStream.tellp());

			outputFileStream.close();
		}

	}

	void runReduceTask() {
		auto reducer = get_reducer_from_task_factory(request_.user_id());

		std::map<std::string,std::vector<std::string>> mapResult;

		for (FileArgs fileArg_ : request_.input_files()) {
			std::ifstream inputFileStream(fileArg_.file_path(), std::ios::in);
			std::string keyString;
			std::string valString;

			while (getline(inputFileStream, keyString) && getline(inputFileStream, valString)){
				mapResult[keyString].push_back(valString);
			}

			inputFileStream.close();
		}

		for (std::map<std::string,std::vector<std::string>>::iterator p = mapResult.begin(); p!=mapResult.end(); ++p){
			reducer->reduce(p->first,p->second);
		}

		std::string outputFilePath = request_.output_dir() + "/" + request_.output_filename();

		std::ofstream outputFileStream(outputFilePath, std::ios::out);

		for (std::pair<std::string,std::string> p : reducer->impl_->reduceResult){
			outputFileStream << p.first << " " << p.second << std::endl;
		}

		FileArgs* newFileArgs = reply_.add_output_files();
		newFileArgs->set_file_path(outputFilePath);
		newFileArgs->set_start_offset(0);
		newFileArgs->set_end_offset(outputFileStream.tellp());

		outputFileStream.close();

	}

	void Proceed() {
		if (status_ == CREATE) {
			
			status_ = PROCESS;

			service_->RequestrunJob(&ctx_, &request_, &responder_, cq_, cq_,
										this);
		} else if (status_ == PROCESS) {

			new CallData(service_, cq_);

			if (request_.map_reduce()){
				runReduceTask();
			}
			else{
				runMapTask();
			}

			reply_.set_job_status(true);

			status_ = FINISH;
			responder_.Finish(reply_, Status::OK, this);
		} else {
			GPR_ASSERT(status_ == FINISH);
			delete this;
		}
	}

	private:

	MasterWorker::AsyncService* service_;

	ServerCompletionQueue* cq_;
	ServerContext ctx_;

	JobRequest request_;
	JobReply reply_;
	ServerAsyncResponseWriter<JobReply> responder_;

	enum CallStatus { CREATE, PROCESS, FINISH };
	CallStatus status_;
};

void Worker::HandleRpcs(){
	new CallData(&service_, cq_.get());
    void* tag;
    bool ok;
    while (true) {
      GPR_ASSERT(cq_->Next(&tag, &ok));
      if (ok)static_cast<CallData*>(tag)->Proceed();
	  else new CallData(&service_, cq_.get());
    }
}

/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	/*  Below 5 lines are just examples of how you will call map and reduce
		Remove them once you start writing your own logic */ 
	// std::cout << "worker.run(), I 'm not ready yet" <<std::endl;
	// auto mapper = get_mapper_from_task_factory("cs6210");
	// mapper->map("I m just a 'dummy', a \"dummy line\"");
	// auto reducer = get_reducer_from_task_factory("cs6210");
	// reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));
	// return true;

	ServerBuilder builder;
    builder.AddListeningPort(ip_addr_port_, grpc::InsecureServerCredentials());

    builder.RegisterService(&service_);

    cq_ = builder.AddCompletionQueue();

    server_ = builder.BuildAndStart();
    std::cout << "Worker listening on " << ip_addr_port_ << std::endl;

	HandleRpcs();

	return true;
}
