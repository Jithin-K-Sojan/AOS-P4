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

     std::cout<<map_kbs<<std::endl;

     long int currShardSize = 0;

     for (auto inputFile: mr_spec.input_files){

          std::ifstream inputFileStream(inputFile, std::ios::in);
          
          inputFileStream.seekg(0,std::ios::end);
          long int inputFileSize = inputFileStream.tellg();

          std::cout<<inputFile<<" size: "<<inputFileSize<<std::endl;

          inputFileStream.seekg(0);

          long int pos = 0;
          
          while (pos<inputFileSize){
               if (currShardSize==0){
                    
                    FileShard newFileShard;
                    fileShards.push_back(newFileShard);
               }


               struct MiniShard newMiniShard;
               
               long int shardSpace = (map_kbs * 1024) - currShardSize;

               if ((inputFileSize-pos)>shardSpace){
                    inputFileStream.seekg(shardSpace,std::ios::cur);
                    std::string temp;

                    // if (inputFileStream.peek()=='\n')std::cout<<"HI!!"<<std::endl;

                    if (getline(inputFileStream, temp)){

                         newMiniShard.fileName = inputFile;
                         newMiniShard.start = pos;

                         if (inputFileStream.tellg()==-1){
                              newMiniShard.end = inputFileSize;
                         }
                         else{
                              newMiniShard.end = inputFileStream.tellg();
                         }

                    }
                    else{
                         // Remove this, will not reach.
                         std::cout<<"Here!"<<std::endl;
                         ; 
                         
                    }
               }
               else{
                    // The file does not end on a "\n"
                    inputFileStream.seekg(0,std::ios::end);
                    newMiniShard.fileName = inputFile;
                    newMiniShard.start = pos;
                    newMiniShard.end = inputFileSize;
               }


               // std::cout<<inputFileStream.tellg()<<std::endl;

               pos = newMiniShard.end;
               //pos+=1;

               currShardSize += (newMiniShard.end - newMiniShard.start + 1);
               fileShards[fileShards.size()-1].miniShards.push_back(newMiniShard);

               if (currShardSize>=(map_kbs*1024)){
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

     // long int end = fileShards[1].miniShards[0].end;
     // std::cout<<end<<std::endl;
     // std::cout<<fileShards[1].miniShards[0].fileName<<std::endl;

     // std::ifstream inputFileStream(fileShards[1].miniShards[0].fileName, std::ios::in);
     // std::string temp;
     // while (inputFileStream.tellg()!=end){

     //      // Can just do while (getline()) at the worker.
     //      if (getline(inputFileStream, temp)){
     //           std::cout<<temp;
     //           if (inputFileStream.tellg()==-1){
     //                std::cout<<"EOF "<<end<<std::endl;
     //                break;
     //           }
     //      }
          
     // }

	return true;
}
