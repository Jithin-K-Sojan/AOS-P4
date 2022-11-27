#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <unordered_map>

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/

		std::vector<std::vector<std::pair<std::string,std::string>>> mapResult;

		int numOfParitions_;

		void setNumOfPartitions(int numOfPartitions);

		int hasher(std::string key);

};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {

}

inline int BaseMapperInternal::hasher(std::string key) {
	int sum = 0;
	for (auto c: key) sum+=(int)c;

	return sum;
}


/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
	// std::cout << "Dummy emit by BaseMapperInternal: " << key << ", " << val << std::endl;

	mapResult[(hasher(key)%numOfParitions_)].push_back(make_pair(key,val));
}

inline void BaseMapperInternal::setNumOfPartitions(int numOfParitions) {
	numOfParitions_ = numOfParitions;
	mapResult.resize(numOfParitions);
}


/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
		BaseReducerInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
	std::cout << "Dummy emit by BaseReducerInternal: " << key << ", " << val << std::endl;
}
