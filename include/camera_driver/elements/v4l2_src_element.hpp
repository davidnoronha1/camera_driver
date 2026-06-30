#pragma once

#include "../pipeline/segment.hpp"
#include "../v4l2_probe.hpp"
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace camera_driver {

// Selection criteria — multiple can be provided; tried in order until one succeeds.
struct BySerial    { std::string serial; };
struct ByPort      { std::string bus_id; };  // matches V4L2 bus_info field
struct ByFormat    { uint32_t fourcc; int width; int height; int fps; };
struct Interactive {};                        // menu-driven, always succeeds

using SelectionCriterion = std::variant<BySerial, ByPort, ByFormat, Interactive>;

// V4L2 camera source element. Always creates an UnresolvedSegment because
// the optimal capture format depends on what downstream elements need.
class V4L2SrcElement : public UnresolvedSegment {
public:
    // criteria: tried in order; first match wins.
    // width/height/fps: desired capture resolution (0 = auto/any).
    V4L2SrcElement(std::vector<SelectionCriterion> criteria,
                   int width = 0, int height = 0, int fps = 30);

    void addCriterion(SelectionCriterion c);

    std::vector<PixelFormat> preferredInputFormats() const override { return {}; }

private:
    std::vector<SelectionCriterion> criteria_;
    int width_, height_, fps_;

    // Built the resolver callback that is passed to UnresolvedSegment
    static std::function<ResolvedSegment(const PipelineContext&)>
        makeResolver(std::vector<SelectionCriterion>* criteria,
                     int* width, int* height, int* fps);
};

} // namespace camera_driver
