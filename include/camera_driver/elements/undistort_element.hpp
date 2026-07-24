#pragma once

#include "../metadata/camera_metadata.hpp"
#include "../pipeline/segment.hpp"
#include <string>

namespace camera_driver {

// Undistorts video using calibration found in Pipeline::scratchpad() (see
// Scratchpad::kCameraMetadataKey) — nothing needs to be passed to this
// element manually; whatever published the calibration (e.g.
// MkvPlaybackElement reading it back from a recording, or code setting it
// directly via pipeline.scratchpad()->set(...)) is picked up automatically.
//
// If no calibration is present at resolve time, this is a pure passthrough
// (contributes nothing to the pipeline string). If present:
//   - GPU path: DeepStream's nvdewarper (projection-type=3, "Perspective to
//     Perspective" — the plain pinhole/radial-tangential model, not the
//     fisheye ones), which requires NVMM RGBA input; a colorspace-convert
//     step is inserted automatically.
//   - CPU fallback: if nvdewarper/nvvidconv aren't available, undistorts via
//     the camera_driver_undistort GstVideoFilter (see gst/undistort_filter.hpp)
//     — a real custom GStreamer element, plain C++ math, no OpenCV (OpenCV's
//     libjpeg collides with nvjpegenc in this process — see CustomPublisher).
class UndistortElement : public UnresolvedSegment {
public:
    UndistortElement();
    ~UndistortElement() override = default;

    void setup(Pipeline* parent) override;

private:
    bool cpu_mode_ = false;
    CalibrationInfo calib_;
};

} // namespace camera_driver
