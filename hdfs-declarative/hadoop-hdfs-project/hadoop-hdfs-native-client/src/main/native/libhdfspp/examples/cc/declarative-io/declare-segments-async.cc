/**
  * An IO declaration example for testing. Declares a set of segments for the given file.
  *
  * Usage: declare-segments <declaration-type> <file-path> <num-segments> <needed> <deadline>
  * built to /hadoop-hdfs-project/hadoop-hdfs-native-client/target/main/native/libhdfspp/examples/cc/declarative-io
  * ./declare-segments-async TEST /test/tmp.txt 4 4 "12:12:12 00:00:00"
  *
  **/

#include "hdfspp/hdfspp.h"
#include "tools_common.h"
#include <google/protobuf/stubs/common.h>
#include <future>
#include <unistd.h>

int main(int argc, char *argv[]) {
  if (argc != 6) {
    std::cerr << "usage: declare-segments-async <declaration-type> <file-path> <num-segments> <needed> <deadline>" << std::endl;
    exit(EXIT_FAILURE);
  }

  std::string declarationType = argv[1];
  std::string path = argv[2];
  unsigned long numSegments = std::stoul(argv[3]);
  unsigned long needed = std::stoul(argv[4]);
  std::string deadline = argv[5];
  bool allowPartialCompletion = true;

  hdfs::URI uri = hdfs::parse_path_or_exit(path);
  std::shared_ptr<hdfs::FileSystem> fs = hdfs::doConnect(uri, true);
  if (!fs) {
    std::cerr << "Could not connect the file system. " << std::endl;
    exit(EXIT_FAILURE);
  }

  std::promise<void> promise;
  std::future<void> future(promise.get_future());
  std::string uuid;
  auto handler = [&promise, &uuid](const hdfs::Status &s, std::string &requestUuid) {
    std::cout << s.ToString() << std::endl;
    uuid = requestUuid;
    promise.set_value();
    return 0;
  };
  std::vector<hdfs::Segment> segments;
  for (unsigned long i = 0; i < numSegments; i++) {
    hdfs::Segment s(path, 0, 0);
    segments.push_back(s);
  }

  std::cout << "Sending request..." << std::endl;
  // Asynchronous call, will get a ClosedChannelException in the NameNode and no response if fs is de-allocated.
  fs->DeclareIOsAsync(declarationType, segments, needed, deadline, allowPartialCompletion, handler);
  // Block until promise is set
  future.get();
  std::cout << "Received request uuid: " << uuid << std::endl;

  std::cout << "Sleeping for a bit..." << std::endl;
  sleep(10);

  std::promise<void> promise2;
  std::future<void> future2(promise2.get_future());
  auto handler2 = [&promise2](const hdfs::Status &s, std::vector<uint64_t> &segmentsSelected) {
      std::cout << s.ToString() << std::endl;
      std::cout << segmentsSelected.size() << std::endl;
      promise2.set_value();
      return 0;
  };

  std::cout << "Polling..." << std::endl;
  // Asynchronous call, will get a ClosedChannelException in the NameNode and no response if fs is de-allocated.
  fs->PollDeclaration(uuid, handler2);
  // Block until promise is set
  future2.get();

  // Clean up static data and prevent valgrind memory leaks
  google::protobuf::ShutdownProtobufLibrary();
  return 0;
}
