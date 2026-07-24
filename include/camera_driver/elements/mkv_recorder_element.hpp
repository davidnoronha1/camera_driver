#pragma once

#include "../metadata/camera_metadata.hpp"
#include "../pipeline/segment.hpp"
#include <gst/gst.h>
#include <optional>
#include <string>

namespace camera_driver {

// Records the incoming video stream to a Matroska (.mkv) file. A sink
// element — plugs into MuxElement branches exactly like MJPEGPublisher /
// NVUnixFDPublisher do, so recording is just another fan-out branch.
//
// Calibration/pose metadata (see CameraMetadata) can be attached via
// setMetadata() at any point before the pipeline reaches EOS; it is written
// into the file as a GST_TAG_COMMENT on the matroskamux element (must be
// GST_TAG_COMMENT, not GST_TAG_EXTENDED_COMMENT — the latter is silently
// dropped by matroskamux, verified empirically). Read it back with
// metadata_io::readMetadataFromFile().
class MkvRecorderElement : public UnresolvedSegment {
public:
    explicit MkvRecorderElement(std::string location);
    ~MkvRecorderElement() override = default;

    bool isSink() const override { return true; }

    void setup(Pipeline* parent) override;
    void bringdown(Pipeline* parent) override;

    // Callable before or after setup()/build() — applied immediately if the
    // muxer element already exists, otherwise applied during setup().
    void setMetadata(const CameraMetadata& metadata);

private:
    std::string location_;
    GstElement* mux_ = nullptr;
    std::optional<CameraMetadata> pending_metadata_;

    void applyMetadata(const CameraMetadata& metadata);
};

} // namespace camera_driver
