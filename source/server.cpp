#include <arpa/inet.h>
#include <cxxopts.hpp>
#include <fmt/format.h>
#include "shared.h"
#include <fcntl.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/text_format.h>
#include "rocksdb/db.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/text_format.h>
#include "message.h"

#include <pthread.h>

bool is_leader;
rocksdb::DB *db;
unsigned server_id;                // id of the server
std::vector<in_port_t> raft_ports; // ports of the raft servers
unsigned term = 0;
unsigned prev_log_index = 0;
unsigned prev_log_term = 0;
unsigned commit_index = 0;

std::vector<int> split(const std::string &s, char delimiter)
{
  std::vector<int> tokens;
  std::string token;
  std::istringstream tokenStream(s);
  while (getline(tokenStream, token, delimiter))
  {
    tokens.push_back(stoi(token));
  }
  return tokens;
}

void *handle_client(void *args)
{
  in_port_t port = *(in_port_t *)args;
  int sockfd = listening_socket(port);
  if (sockfd < 0)
  {
    perror("Listening socket failed\n");
    exit(1);
  }
  
  int newsockfd = accept_connection(sockfd);
  while (newsockfd != -1)
  {

    if (newsockfd < 0)
    {
      perror("Accepting connection failed\n");
      exit(1);
    }
    
    auto [bytecount, buffer] = secure_recv(newsockfd);
    if (bytecount <= 0)
    {
      break;
    }
    if (buffer == nullptr || bytecount == 0)
    {
      return nullptr;
    }
    /* Parsing the message from the buffer */
    kvs::client_msg request;
    auto size = bytecount;
    std::string client_message(buffer.get(), size);
    request.ParseFromString(client_message);
    kvs::server_response response;
    std::string response_string;

    raft::AppendEntriesRequest replicate_request;
    std::string raft_string;
    replicate_request.set_leaderid(server_id);
    replicate_request.set_term(term);
    replicate_request.set_prevlogindex(prev_log_index);
    replicate_request.set_prevlogterm(prev_log_term);
    replicate_request.set_leadercommit(commit_index);
    raft::Entry* entry = replicate_request.add_entries();
    *entry->mutable_msg() = request;
        
    if (request.type() != kvs::client_msg::DIRECT_GET && is_leader == false)
    {
      // Send Not leader message to client
      response.set_status(kvs::server_response::NOT_LEADER);
    }
    else if (request.type() == kvs::client_msg::DIRECT_GET || request.type() == kvs::client_msg::GET)
    {
      std::string key = request.key();
      std::string value;
      auto status = db->Get(rocksdb::ReadOptions(), key, &value);
      if (status.ok())
      {
        response.set_status(kvs::server_response::OK);
        response.set_value(value);
      }
      else
      {
        response.set_status(kvs::server_response::NO_KEY);
      }
    }
    else if (request.type() == kvs::client_msg::PUT)
    {
      std::string key = request.key();
      std::string value = request.value();
      auto status = db->Put(rocksdb::WriteOptions(), key, value);
      
      if (status.ok())
      {
        response.set_status(kvs::server_response::OK);
        replicate_request.SerializeToString(&raft_string);
        // connect to each server and send message
        for (int i=0; i< raft_ports.size(); i++){
          in_port_t server_port = raft_ports[i];
          if(i == server_id){
            continue;
          }
          
          if(server_port == port ) continue;
          int replicatefd = connect_socket("localhost", server_port);
          if (replicatefd < 0)
          {
            perror("Connecting socket failed\n");
            exit(1);
          }
          secure_send_message(replicatefd, raft_string);
          
        }
      }
      else
      {
        response.set_status(kvs::server_response::ERROR);
      }
    }
    
    response.set_id(server_id);
    response.SerializeToString(&response_string);
    secure_send_message(newsockfd, response_string);
    
    
    newsockfd = accept_connection(sockfd);
  }
  close(sockfd);
  close(newsockfd);
  return nullptr;
}

void *handle_servers(void *args)
{
  in_port_t port = *(in_port_t *)args;
  int sockfd = listening_socket(port);
  if (sockfd < 0)
  {
    perror("Listening socket failed\n");
    exit(1);
  }
  while (true)
  {
    int newsockfd = accept_connection(sockfd);
    
    if (newsockfd < 0)
    {
      perror("Accepting connection failed\n");
      exit(1);
    }

    // Receive message from another server
    auto [bytecount, buffer] = secure_recv(newsockfd);
    if (bytecount <= 0)
    {
      break;
    }
    if (buffer == nullptr || bytecount == 0)
    {
      return nullptr;
    }
    
    // Parse the message from the buffer
    raft::AppendEntriesRequest request;
    auto size = bytecount;
    std::string raft_string(buffer.get(), size);
    request.ParseFromString(raft_string);
    std::string key = request.entries(0).msg().key();
    std::string value = request.entries(0).msg().value();
    // Write to the database
    auto status = db->Put(rocksdb::WriteOptions(), key, value);
    assert(status.ok());
    
    /*
    // Check if the message is valid
    if (request.term() < term)
    {
      // Send Not leader message to client
      kvs::server_response response;
      response.set_status(kvs::server_response::NOT_LEADER);
      response.set_id(server_id);
      response.SerializeToString(&raft_string);
      secure_send_message(newsockfd, raft_string);
      continue;
    }*/
    /*
    // Update the state of the server
    if (request.term() > term)
    {
      term = request.term();
      is_leader = false;
    }
    if (request.term() == term)
    {
      if (request.leaderid() > server_id)
      {
        server_id = request.leaderid();
        is_leader = false;
      }
      if (request.leaderid() == server_id)
      {
        if (request.prevlogindex() > prev_log_index)
        {
          prev_log_index = request.prevlogindex();
          prev_log_term = request.prevlogterm();
          commit_index = request.leadercommit();
        }
      }
    }*/
  }
  return nullptr;
}

auto main(int argc, char *argv[]) -> int
{
  rocksdb::Options opts;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  opts.IncreaseParallelism();
  opts.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  opts.create_if_missing = true;
  opts.compression_per_level.resize(opts.num_levels);
#if 1
  for (int i = 0; i < opts.num_levels; i++)
  {
    opts.compression_per_level[i] = rocksdb::kNoCompression;
  }
#endif
  opts.compression = rocksdb::kNoCompression;
  std::string db_path = "./db" + std::to_string(getpid());
  rocksdb::Status status = rocksdb::DB::Open(opts, db_path, &db);
  assert(status.ok());

  cxxopts::Options options("svr", "server for cloud-lab task 3");
  options.allow_unrecognised_options().add_options()(
      "p,port", "Port of the server", cxxopts::value<in_port_t>())(
      "l,leader", "Leader", cxxopts::value<bool>()->default_value("false"))(
      "r,raft_ports", "Raft ports", cxxopts::value<std::vector<in_port_t>>())(
      "i,server_id",
      "Server id",
      cxxopts::value<unsigned>()->default_value("0"))("h,help", "Print help");

  auto args = options.parse(argc, argv);

  if (args.count("help"))
  {
    fmt::print("{}\n", options.help());
    return 0;
  }

  if (!args.count("port"))
  {
    fmt::print(stderr, "The port is required\n{}\n", options.help());
    return -1;
  }

  // TODO: implement!
  if (!args.count("raft_ports"))
  {
    fmt::print(stderr, "The raft ports are required\n{}\n", options.help());
    return -1;
  }
  if (!args.count("server_id"))
  {
    fmt::print(stderr, "The server id is required\n{}\n", options.help());
    return -1;
  }

  in_port_t port = args["port"].as<in_port_t>();
  raft_ports = args["raft_ports"].as<std::vector<in_port_t>>();
  server_id = args["server_id"].as<unsigned>();
  is_leader = args["leader"].as<bool>();
  in_port_t rpc_port = raft_ports[server_id];

  // Create two threads to listen two different ports
  pthread_t cli_thread;
  pthread_create(&cli_thread, NULL, handle_client, (void *)&port);
  pthread_t svr_thread;
  pthread_create(&svr_thread, NULL, handle_servers, (void *)&rpc_port);

  // Close pthreads
  pthread_join(cli_thread, NULL);
  pthread_join(svr_thread, NULL);

  return 0;
}
