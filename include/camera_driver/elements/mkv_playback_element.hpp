#pragma once

#include "../pipeline/segment.hpp"
#include <string>

namespace camera_driver {

// Reads back an .mkv file written by MkvRecorderElement as a pipeline
// source: demuxes + hardware-decodes H264 to NV12(_NVMM) (falls back to
// software avdec_h264 if no NVIDIA decoder is available).
//
// Any calibration/pose embedded by MkvRecorderElement's setMetadata() is
// read (via metadata_io::readMetadataFromFile) and published to
// Pipeline::scratchpad() under Scratchpad::kCameraMetadataKey — downstream
// elements like UndistortElement pick it up automatically, no manual wiring
// required. This happens inside the resolver (not setup()) specifically so
// it runs during Pipeline::build()'s resolveSegments() pass, before later
// elements' resolvers run — setup() runs too late (after every element has
// already resolved its gst string).
//
// Because of that ordering requirement, MkvPlaybackElement must be added to
// the Pipeline before any element that reads calibration from the
// scratchpad (e.g. UndistortElement).
class MkvPlaybackElement : public UnresolvedSegment {
public:
    explicit MkvPlaybackElement(std::string location);
    ~MkvPlaybackElement() override = default;

private:
    std::string location_;
};

} // namespace camera_driver
