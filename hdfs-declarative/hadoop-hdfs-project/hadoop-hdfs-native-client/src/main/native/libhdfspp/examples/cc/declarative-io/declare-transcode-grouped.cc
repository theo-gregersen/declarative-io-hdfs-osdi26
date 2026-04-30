/**
  * Transcode a list of files using declarative IO and the HDFS API.
  **/

#include "hdfspp/hdfspp.h"
#include "tools_common.h"
#include <google/protobuf/stubs/common.h>
#include <future>
#include <stdlib.h>
#include <unistd.h>

int main(int argc, char *argv[]) {
  if (argc < 6) {
    std::cerr << "usage: declare-transcode-grouped <destination> <deadline> <poll period> <max files per cp> <path1> <path2> ... <pathn>" << std::endl;
    exit(EXIT_FAILURE);
  }

  const char *hadoop_home_env = getenv("HADOOP_HOME");
  if (hadoop_home_env == nullptr) {
    std::cerr << "environment variable $HADOOP_HOME is not set" << std::endl;
    exit(EXIT_FAILURE);
  }
  std::string hadoop_home = hadoop_home_env;

  std::string transcode_path = argv[1];
  std::string deadline = argv[2];
  std::string declarationType = "TRANSCODE";
  bool allowPartialCompletion = true;

  hdfs::URI uri = hdfs::parse_path_or_exit(argv[1]);
  std::shared_ptr<hdfs::FileSystem> fs = hdfs::doConnect(uri, true);
  if (!fs) {
    std::cerr << "Could not connect the file system. " << std::endl;
    exit(EXIT_FAILURE);
  }

  unsigned long pollPeriod = std::stoul(argv[3]);
  unsigned long maxFiles = std::stoul(argv[4]);

  std::promise<void> dec_promise;
  std::future<void> dec_future(dec_promise.get_future());
  std::string uuid;
  std::vector<std::string> paths; // shove these on heap
  unsigned long num_done = 0;

  // Make the declaration and get the uuid.
  std::vector<hdfs::Segment> segments;
  for (int i = 5; i < argc; i++) {
    hdfs::Segment s(argv[i], 0, 1024*1024*1024);
    segments.push_back(s);
    paths.push_back(argv[i]);
  }
  auto handler = [&dec_promise, &uuid](const hdfs::Status &s, std::string &requestUuid) {
    std::cout << requestUuid << std::endl;
    uuid = requestUuid;
    dec_promise.set_value();
    return 0;
  };
  fs->DeclareIOsAsync(declarationType, segments, segments.size(), deadline, allowPartialCompletion, handler);
  dec_future.get();

  // Continuously poll until there are no more paths to transcode.
  while (num_done != paths.size()) {
    std::promise<void> poll_promise;
    std::future<void> poll_future(poll_promise.get_future());
    std::vector<std::string> pathsToCopy;

    auto handler = [&poll_promise, &pathsToCopy, &paths](const hdfs::Status &s, std::vector<uint64_t> &segmentsSelected) {
      if (segmentsSelected.size() != 0) {
        for (size_t i = 0; i < segmentsSelected.size(); i++) {
          size_t j = segmentsSelected[i];
          pathsToCopy.push_back(paths[j]);
        }
      }
      poll_promise.set_value();
      return 0;
    };
    fs->PollDeclaration(uuid, handler);
    poll_future.get();

    if (!pathsToCopy.empty()) {
      num_done += pathsToCopy.size();
      std::string allPaths;
      size_t i = 0;

      while (i < pathsToCopy.size()) {
        allPaths = "";
        if (maxFiles != 0) {
          for (size_t j = 0; i < pathsToCopy.size() && j < maxFiles; j++) {
            allPaths += pathsToCopy[i] + " ";
            i++;
          }
        } else {
          for (size_t j = 0; j < pathsToCopy.size(); j++) {
            allPaths += pathsToCopy[j] + " ";
            i++;
          }
        }

        std::string cmd = "(HADOOP_ROOT_LOGGER=INFO,console " +
          hadoop_home + "/bin/hadoop distcp -overwrite -skipcrccheck " + allPaths + " " + transcode_path + " && " +
          hadoop_home + "/bin/hdfs dfs -rm " + allPaths + ") &";
        int cmdStatus = system(cmd.c_str());
        if (cmdStatus != 0) {
          std::cout << "Transcoding error: failed to transcode " + allPaths << std::endl;
        }
        std::cout << "Finished transcoding batch" << std::endl;
      }
      pathsToCopy.clear();
    }

    sleep(pollPeriod);
  }

  google::protobuf::ShutdownProtobufLibrary();
  return 0;
}
