/**
  * An IO declaration example for testing. Declares a set of segments for the given file.
  *
  * Usage: declare-segments <declaration-type> <file-path> <num-segments> <needed> <deadline>
  *
  **/

#include "hdfspp/hdfspp.h"
#include "tools_common.h"
#include <google/protobuf/stubs/common.h>
#include <future>

int main(int argc, char *argv[]) {
  if (argc != 6) {
    std::cerr << "usage: declare-segments <declaration-type> <file-path> <num-segments> <needed> <deadline>" << std::endl;
    exit(EXIT_FAILURE);
  }

  std::string declarationType = argv[1];
  std::string path = argv[2];
  unsigned long numSegments = std::stoul(argv[3]);
  unsigned long needed = std::stoul(argv[4]);
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

  auto handler = [&promise](const hdfs::Status &s, std::vector<uint64_t> &segmentsSelected) {
    std::cout << s.ToString() << std::endl;
    std::cout << segmentsSelected.size() << std::endl;
    promise.set_value();
    return 0;
  };

  std::vector<hdfs::Segment> segments;
  for (unsigned long i = 0; i < numSegments; i++) {
    hdfs::Segment s(path, 0, 0);
    segments.push_back(s);
  }

  // Asynchronous call, will get a ClosedChannelException in the NameNode and no response if fs is de-allocated.
  fs->DeclareIOs(declarationType, segments, needed, deadline, handler);

  // Block until promise is set
  future.get();

  // Clean up static data and prevent valgrind memory leaks
  google::protobuf::ShutdownProtobufLibrary();
  return 0;
}
