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
unsigned server_id; // id of the server

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
  rocksdb::Status status = rocksdb::DB::Open(opts, "./testdb", &db);
  assert(status.ok());

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
    std::cout << "Accepted connection from " << newsockfd << std::endl;
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
        // send appendentries to all other servers
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
      }
      else
      {
        kvs::server_response response;
        response.set_status(kvs::server_response::ERROR);
      }
    }
    response.set_id(server_id);
    response.SerializeToString(&response_string);
    secure_send_message(newsockfd, response_string);
  }
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
    std::cout << "Accepted connection from " << newsockfd << std::endl;
  }
  std::cout << "Thread is closing" << std::endl;
  return nullptr;
}

auto main(int argc, char *argv[]) -> int
{
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
  std::vector<in_port_t> raft_ports = args["raft_ports"].as<std::vector<in_port_t>>();
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
