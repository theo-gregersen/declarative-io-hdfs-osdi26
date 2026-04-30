/**
  * An IO declaration example for testing.
  *
  * Usage: declare <declaration-type> <file-path> <byte-offset> <num-bytes> <deadline>
  *
  **/

#include "hdfspp/hdfspp.h"
#include "tools_common.h"
#include <google/protobuf/stubs/common.h>
#include <future>

int main(int argc, char *argv[]) {
  if (argc != 6) {
    std::cerr << "usage: declare <declaration-type> <file-path> <byte-offset> <num-bytes> <deadline>" << std::endl;
    exit(EXIT_FAILURE);
  }

  std::string declarationType = argv[1];
  std::string path = argv[2];
  unsigned long offset = std::stoul(argv[3]);
  unsigned long length = std::stoul(argv[4]);
  std::string deadline = argv[5];

  hdfs::URI uri = hdfs::parse_path_or_exit(path);
  std::shared_ptr<hdfs::FileSystem> fs = hdfs::doConnect(uri, true);
  if (!fs) {
    std::cerr << "Could not connect the file system. " << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << "Sending request..." << std::endl;

  std::promise<void> promise;
  std::future<void> future(promise.get_future());

  auto handler = [&promise](const hdfs::Status &s) {
    std::cout << s.ToString() << std::endl;
    promise.set_value();
    return 0;
  };

  // Asynchronous call, will get a ClosedChannelException in the NameNode and no response if fs is de-allocated.
  fs->DeclareIO(declarationType, path, offset, length, deadline, handler);

  // Block until promise is set
  future.get();

  // Clean up static data and prevent valgrind memory leaks
  google::protobuf::ShutdownProtobufLibrary();
  return 0;
}
