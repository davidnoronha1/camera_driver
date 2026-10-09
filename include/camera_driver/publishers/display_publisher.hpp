#pragma once

#include "../pipeline/segment.hpp"
#include <string>

namespace camera_driver {

// Debugging sink: decodes/converts incoming frames and shows them in an on-screen
// window via GStreamer's fpsdisplaysink.
class DisplayPublisher : public UnresolvedSegment {
public:
    explicit DisplayPublisher(std::string window_name = "camera_driver");
    ~DisplayPublisher() override = default;

    bool isSink() const override { return true; }

private:
    std::string window_name_;
};

} // namespace camera_driver
