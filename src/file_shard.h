#pragma once

#include <vector>
#include "mapreduce_spec.h"

//ADDED
#include <fstream>


// Created an additioinal structure for ease of moving complete shards around.
struct MiniShard{
     std::string fileName;
     long int start;
     long int end;
};

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {
     std::vector<MiniShard> miniShards; 
};


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {

     long int map_kbs = mr_spec.map_kilobytes;

     for (auto inputFile: mr_spec.input_files){

          // if (currShardSize==0){
          //      struct FileShard newFileShard;
          //      fileShards.push_back(newFileShard);
          // }

          std::ifstream inputFileStream(inputFile);

          inputFileStream.seekg(0,std::ifstream::end);
          long int inputFileSize = inputFileStream.tellg();

          // std::cout<<inputFile<<" size: "<<inputFileSize<<std::endl;

          inputFileStream.seekg(0);

          long int currShardSize = 0;
          long int pos = 0;
          
          while (pos<inputFileSize){
               if (currShardSize==0){
                    
                    FileShard newFileShard;
                    fileShards.push_back(newFileShard);
               }


               struct MiniShard newMiniShard;
               if ((inputFileSize-pos)>(map_kbs*1024)){
                    inputFileStream.seekg((map_kbs*1024),std::ifstream::cur);
                    std::string temp;
                    // Should we peek and check if it is at '\n'? Or does getline return empty in that case?

                    if (getline(inputFileStream, temp)){

                         newMiniShard.fileName = inputFile;
                         newMiniShard.start = pos;
                         newMiniShard.end = inputFileStream.tellg();

                    }
                    else{
                         ; // This case is probably not possible.


                    }
               }
               else{
                    // The file has to end on a '\n' right?
                    newMiniShard.fileName = inputFile;
                    newMiniShard.start = pos;
                    newMiniShard.end = inputFileSize;
               }


               std::cout<<inputFileStream.tellg()<<std::endl;

               pos = inputFileStream.tellg();

               currShardSize += (newMiniShard.end - newMiniShard.start);
               fileShards[fileShards.size()-1].miniShards.push_back(newMiniShard);

               if (currShardSize>(map_kbs*1024)){
                    currShardSize = 0;
               }

          }

          inputFileStream.close();

     }

     for (auto i: fileShards){
          std::cout<<"New Shard\n";
          for (auto j: i.miniShards){
               std::cout<<j.fileName<<" "<<j.start<<" "<<j.end<<std::endl;
          }
     }

	return true;
}
