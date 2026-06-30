#pragma once

#include "../pipeline/segment.hpp"
#include "optimized_video_resize.hpp"
#include <optional>
#include <vector>

namespace camera_driver {

// Hardware-aware format converter and encoder.
// Picks the best GST conversion/encoding path based on detected hardware.
// Requests optimal input format from upstream (V4L2SrcElement synergy).
// Always an UnresolvedSegment — resolved at Pipeline::build() time.
class OptimizedConverter : public UnresolvedSegment {
public:
    // desired_outputs: target formats in preference order (first = most preferred).
    // resize: optional hint to fold a resize into the same hardware step.
    // quality: JPEG/MJPEG encode quality (1-100).
    // bitrate_kbps: H.264 target bitrate in kbps (0 = encoder default).
    OptimizedConverter(std::vector<PixelFormat> desired_outputs,
                       std::optional<OptimizedVideoResize> resize = std::nullopt,
                       int quality = 85,
                       int bitrate_kbps = 0);

    std::vector<PixelFormat> preferredInputFormats() const override;

private:
    std::vector<PixelFormat> desired_outputs_;
    std::optional<OptimizedVideoResize> resize_;
    int quality_;
    int bitrate_kbps_;

    static ResolvedSegment buildH264Path(PixelFormat input, const PipelineContext& ctx,
                                         int bitrate_kbps, const std::optional<OptimizedVideoResize>& resize);
    static ResolvedSegment buildMJPEGPath(PixelFormat input, const PipelineContext& ctx,
                                          int quality, const std::optional<OptimizedVideoResize>& resize);
    static std::string resizeStr(const OptimizedVideoResize& r);
};

} // namespace camera_driver
