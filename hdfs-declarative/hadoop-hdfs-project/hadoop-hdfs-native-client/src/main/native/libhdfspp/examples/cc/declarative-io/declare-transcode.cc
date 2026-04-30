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
    std::cerr << "usage: declare-transcode <destination> <deadline> <poll period> <max files per cp> <path1> <path2> ... <pathn>" << std::endl;
    exit(EXIT_FAILURE);
  }

  const char *hadoop_home_env = getenv("HADOOP_HOME");
  if (hadoop_home_env == nullptr) {
    std::cerr << "environment variable $HADOOP_HOME is not set" << std::endl;
    exit(EXIT_FAILURE);
  }
  std::string hadoop_home = hadoop_home_env;

  std::string deadline = argv[2];
  std::string declarationType = "TRANSCODE";
  bool allowPartialCompletion = false;

  hdfs::URI uri = hdfs::parse_path_or_exit(argv[1]);
  std::shared_ptr<hdfs::FileSystem> fs = hdfs::doConnect(uri, true);
  if (!fs) {
    std::cerr << "Could not connect the file system. " << std::endl;
    exit(EXIT_FAILURE);
  }

  unsigned long pollPeriod = std::stoul(argv[3]);
  unsigned long maxFiles = std::stoul(argv[4]);

  std::vector<std::future<void>> futures;
  std::vector<std::string> paths; // shove these on heap
  std::vector<std::string> uuids;

  // Make the declarations and gather the uuids.
  for (int i = 5; i < argc; i++) {
    std::promise<void> promise;
    std::future<void> future(promise.get_future());

    std::vector<hdfs::Segment> segments;
    hdfs::Segment s(argv[i], 0, 1024*1024*1024);
    segments.push_back(s);
    paths.push_back(argv[i]);
    auto handler = [&promise, &uuids](const hdfs::Status &s, std::string &requestUuid) {
      std::cout << requestUuid << std::endl;
      uuids.push_back(requestUuid);
      promise.set_value();
      return 0;
    };

    fs->DeclareIOsAsync(declarationType, segments, 1, deadline, allowPartialCompletion, handler);
    future.get();
  }

  // Continuously poll until there are no more uuids left.
  while (!uuids.empty()) {
		std::vector<std::string> pathsToCopy;
    for (int i = uuids.size()-1; i >= 0; i--) {
      std::promise<void> promise;
      std::future<void> future(promise.get_future());
      bool go_ahead = false;
      std::string uuid = uuids[i];

      auto handler = [&promise, &go_ahead](const hdfs::Status &s, std::vector<uint64_t> &segmentsSelected) {
          if (segmentsSelected.size() != 0) {
            go_ahead = true;
          }
          promise.set_value();
          return 0;
      };

      fs->PollDeclaration(uuid, handler);
      future.get();

      if (go_ahead) {
        std::string path = paths[i];
        paths.erase(paths.begin() + i);
        uuids.erase(uuids.begin() + i);
				pathsToCopy.push_back(path);
      }
    }

    // Potentially want to use -cp and pool scheduled files into batches to avoid overloading client memory (-cp temporarily writes clientside)
    // Trying distcp which doesn't bring any data clientside
		if (!pathsToCopy.empty()) {
      std::string transcode_path = argv[1];
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
//        std::string command =
//          "(HADOOP_ROOT_LOGGER=INFO,console " +
//          hadoop_home + "/bin/hdfs dfs -cp -p " + allPaths + " " + transcode_path + "; " +
//          hadoop_home + "/bin/hdfs dfs -rm " + allPaths + ")";
        // std::string command =
        //    "(HADOOP_ROOT_LOGGER=INFO,console " +
        //    hadoop_home + "/bin/hadoop distcp -skipcrccheck " + allPaths + " " + transcode_path + "; " +
        //    hadoop_home + "/bin/hdfs dfs -rm " + allPaths + ")";
        // std::cout << command << std::endl;
        // system(command.c_str());
        std::string cmd1 = "HADOOP_ROOT_LOGGER=INFO,console " + hadoop_home + "/bin/hadoop distcp -skipcrccheck " + allPaths + " " + transcode_path;
        int cpStatus = system(cmd1.c_str());
        if (cpStatus != 0) {
          std::cout << "Transcoding error: failed to copy " + allPaths << std::endl;
        } else {
          std::string cmd2 = hadoop_home + "/bin/hdfs dfs -rm " + allPaths;
          int rmStatus = system(cmd2.c_str());
          if (rmStatus != 0) {
            std::cout << "Transcoding error: failed to remove " + allPaths << std::endl;
          }
        }
        std::cout << "finished transcoding batch" << std::endl;
      }
      pathsToCopy.clear();
    }

    sleep(pollPeriod);
  }

  google::protobuf::ShutdownProtobufLibrary();
  return 0;
}

/**
HADOOP_ROOT_LOGGER=INFO,console ${HADOOP_HOME}/bin/hdfs dfs -cp -f /tmp/dfs-perf-workspace/simple-read-write/0/1-98 /tmp/dfs-perf-workspace/simple-read-write/0/1-97 /tmp/dfs-perf-workspace/simple-read-write/0/1-96 /tmp/dfs-perf-workspace/simple-read-write/0/1-95 /tmp/dfs-perf-workspace/simple-read-write/0/1-94 /tmp/dfs-perf-workspace/simple-read-write/0/1-93 /tmp/dfs-perf-workspace/simple-read-write/0/1-92 /tmp/dfs-perf-workspace/simple-read-write/0/1-91 /.transcoded
HADOOP_ROOT_LOGGER=INFO,console ${HADOOP_HOME}/bin/hadoop distcp -skipcrccheck /tmp/dfs-perf-workspace/simple-read-write/0/1-98 /tmp/dfs-perf-workspace/simple-read-write/0/1-97 /tmp/dfs-perf-workspace/simple-read-write/0/1-96 /tmp/dfs-perf-workspace/simple-read-write/0/1-95 /tmp/dfs-perf-workspace/simple-read-write/0/1-94 /tmp/dfs-perf-workspace/simple-read-write/0/1-93 /tmp/dfs-perf-workspace/simple-read-write/0/1-92 /tmp/dfs-perf-workspace/simple-read-write/0/1-91 /.transcoded

*/
