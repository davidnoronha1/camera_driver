#pragma once

#include "caps.hpp"
#include "../hw_detect.hpp"
#include "../v4l2_probe.hpp"
#include <vector>

namespace camera_driver {

class Pipeline;

struct PipelineContext {
    // Hardware capabilities detected at build() time.
    hw::HWCaps hw;

    // What the previous (upstream) segment outputs.
    Caps upstream_caps;

    // What the next element prefers to receive (ordered, first = most preferred).
    std::vector<PixelFormat> downstream_prefs;

    // Available V4L2 formats from the upstream V4L2SrcElement (if applicable).
    // Populated by V4L2SrcElement so OptimizedConverter can check native formats.
    std::vector<v4l2probe::FormatInfo> v4l2_formats;

    // Reference to the owning pipeline (for advanced resolvers).
    Pipeline* pipeline = nullptr;
};

} // namespace camera_driver
