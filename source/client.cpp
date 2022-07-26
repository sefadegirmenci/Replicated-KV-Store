#include <iostream>

#include <arpa/inet.h>
#include <cxxopts.hpp>
#include <fmt/format.h>
#include <iostream>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <cxxopts.hpp>
#include <fcntl.h>
#include <fstream>
#include <fmt/core.h>
#include "message.h"
#include "shared.h"
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/text_format.h>

auto main(int argc, char *argv[]) -> int
{
  cxxopts::Options options(argv[0], "Client for the task 3");
  options.allow_unrecognised_options().add_options()(
      "p,port", "Port of the server", cxxopts::value<in_port_t>())(
      "o,operation",
      "Specify either a GET, PUT or DIRECT_GET operation",
      cxxopts::value<std::string>())(
      "k,key", "Specify key for an operation", cxxopts::value<std::string>())(
      "v,value",
      "Specify value for a PUT operation",
      cxxopts::value<std::string>())("h,help", "Print help");

  auto args = options.parse(argc, argv);
  if (args.count("help"))
  {
    fmt::print("{}\n", options.help());
    return 0;
  }

  if (!args.count("port"))
  {
    fmt::print(stderr, "The port is required\n{}\n", options.help());
    return 1;
  }

  if (!args.count("operation"))
  {
    fmt::print(stderr, "The operation is required\n{}\n", options.help());
    return 1;
  }
  auto operation = args["operation"].as<std::string>();
  if (operation != "GET" && operation != "PUT" && operation != "DIRECT_GET")
  {
    fmt::print(stderr, "The invalid operation: {}\n", operation);
    return 1;
  }

  if (!args.count("key"))
  {
    fmt::print(stderr, "The key is required\n{}\n", options.help());
    return 1;
  }

  std::string value;
  if (!args.count("value"))
  {
    if (operation == "PUT")
    {
      fmt::print(stderr, "The value is required\n{}\n", options.help());
      return 1;
    }
  }
  else
  {
    value = args["value"].as<std::string>();
  }

  auto port = args["port"].as<in_port_t>();
  auto key = args["key"].as<std::string>();

  // TODO: implement!
  auto ret = 0;
  int sockfd = connect_socket("localhost", port);
  if (sockfd < 0)
  {
    perror("Error creating socket");
  }
  kvs::client_msg request;
  kvs::server_response response;
  std::string request_str;
  std::string response_str;

  request.set_id(0);
  if (operation == "GET")
  {
    request.set_type(kvs::client_msg::GET);
  }
  else if (operation == "PUT")
  {
    request.set_type(kvs::client_msg::PUT);
    request.set_value(value);
  }
  else if (operation == "DIRECT_GET")
  {
    request.set_type(kvs::client_msg::DIRECT_GET);
  }
  request.set_key(key);
  request.SerializeToString(&request_str);
  secure_send_message(sockfd, request_str);
  // receive the message from server
  auto [bytecount, buffer] = secure_recv(sockfd);
  std::cout << "Received the response" << std::endl;
  if (bytecount <= 0)
  {
    return 1;
  }
  if (buffer == nullptr || bytecount == 0)
  {
    return 1;
  }
  kvs::server_response server_resp;
  auto size = bytecount;
  std::string response_string(buffer.get(), size);
  server_resp.ParseFromString(response_string);
  std::cout << "The response is: " << server_resp.DebugString() << std::endl;
  if(server_resp.status() == kvs::server_response::NO_KEY) {
    return 3;
  }
  else if(server_resp.status () == kvs::server_response::NOT_LEADER) {
    return 2;
  }
  else if(server_resp.status () == kvs::server_response::ERROR) {
    return 4;
  }
  else if (server_resp.status() == kvs::server_response::OK){
    if(operation == "GET") {
      std::cout << server_resp.value() << std::endl;
    }
  } else {
    return 1;
  }

  // Exit code
  // 0 -> OK
  // 1 -> Client error
  // 2 -> Not leader
  // 3 -> No key
  // 4 -> Server error
  close(sockfd);
  return ret;
}
