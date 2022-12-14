#pragma once

#include <string>

//ADDED
#include <fstream>
#include <sstream>
#include <iostream>
#include <vector>


/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
	int n_workers;
	std::vector<std::string> worker_ipaddr_ports;
	std::vector<std::string> input_files;
	std::string output_dir;
	int n_output_files;
	long int map_kilobytes;
	std::string user_id;
};

// ADDED
inline std::string trim_string(std::string str){
	int n = str.size();

	int i;
	for (i = 0;i<str.size();i++){
		if (str[i]!=' ') break;
	}
	std::string ltrimmed = str.substr(i);

	while(ltrimmed[ltrimmed.size()-1]==' '){
		ltrimmed.pop_back();
	}

	return ltrimmed;
}

/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {

	// Using ifstream: https://cplusplus.com/doc/tutorial/files/
	std::string line;
	std::ifstream configFile(config_filename);

	if (configFile.is_open()){
		while (getline(configFile,line)){
			
			// Splitting the line based on the key and value.
			std::stringstream kvStream(line);
			std::string Key,Value;

			if (getline(kvStream, Key, '=')){
				
				std::string key = trim_string(Key);

				if (!getline(kvStream, Value, '=')){
					
					std::cout<<"No value present for key "<<key<<" in config file.\n";
					configFile.close();
					return false;
				}

				std::string value = trim_string(Value);

				if (key=="n_workers"){
					mr_spec.n_workers = stoi(value);
				}
				else if (key=="worker_ipaddr_ports"){
					std::stringstream addressStream(value);
					std::string address;

					while (getline(addressStream, address, ',')){
						mr_spec.worker_ipaddr_ports.push_back(address);
					}
				}
				else if (key=="input_files"){
					std::stringstream inputFileStream(value);
					std::string inputFile;

					while (getline(inputFileStream, inputFile, ',')){
						mr_spec.input_files.push_back(inputFile);
					}
				}
				else if (key=="output_dir"){
					mr_spec.output_dir = value;
				}
				else if (key=="n_output_files"){
					mr_spec.n_output_files = stoi(value);
				}
				else if (key=="map_kilobytes"){
					mr_spec.map_kilobytes = stol(value);
				}
				else if (key=="user_id"){
					mr_spec.user_id = value;
				}
				else{
					std::cout<<"Undefined key "<<key<<" set in config file "<<config_filename<<" \n";
					configFile.close();
					return false;
				}

			}

		}
	}
	configFile.close();

	return true;
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {

	if (mr_spec.n_workers > mr_spec.worker_ipaddr_ports.size()){
		std::cout<<"Number of specified worker addresses less than n_workers in the configuration!\n";
		return false;
	}

	for (auto fileName: mr_spec.input_files){
		std::ifstream inputFile(fileName);
		if(!inputFile.good()){
			std::cout<<"Input file "<<fileName<<" not found!\n";
			inputFile.close();
			return false;
		}
		inputFile.close();
	}

	return true;
}
