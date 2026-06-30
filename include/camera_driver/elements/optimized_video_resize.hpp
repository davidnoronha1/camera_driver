#pragma once

namespace camera_driver {

// Hint passed to OptimizedConverter to perform resize in the same hardware step.
// Not a PipelineElement — attach it to OptimizedConverter's constructor.
struct OptimizedVideoResize {
    int width  = 0;
    int height = 0;
};

} // namespace camera_driver
