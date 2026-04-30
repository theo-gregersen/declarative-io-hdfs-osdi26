// ======== IO PLANNER EXTENSIONS ======== //

#ifndef HDFSPP_SEGMENT_H
#define HDFSPP_SEGMENT_H

namespace hdfs {

/**
 * Represents the set of bytes from offset to offset+length in a file
 **/
class Segment {
  public:
    std::string path;
    uint64_t offset;
    uint64_t length;

    Segment(std::string p, uint64_t o, uint64_t l) {
      path = p;
      offset = o;
      length = l;
    }
};

}

#endif

// ======================================= //