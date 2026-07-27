#pragma once

#include "../pipeline/segment.hpp"
#include <string>

namespace camera_driver {

// Explicit video conversion element. The user places this deliberately to
// acknowledge that a format conversion is happening (and its latency cost).
// Prevents the "Caps mismatch — add AutoVideoConverterElement()" error.
// If upstream is a Bayer format, prepends bayer2rgb (videoconvert alone
// can't debayer). If target is set, appends a caps filter.
class AutoVideoConverterElement : public UnresolvedSegment {
public:
    explicit AutoVideoConverterElement(PixelFormat target = PixelFormat::Unknown);

    void setup(Pipeline* parent) override;

private:
    PixelFormat target_;
};

} // namespace camera_driver
