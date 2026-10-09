#pragma once

#include "../pipeline/pipeline.hpp"
#include <string>
#include <yaml-cpp/yaml.h>

namespace camera_driver {

// Constructs a Pipeline (or RTSPPipeline) from a YAML configuration file.
// Uses ElementRegistry to instantiate elements — no per-type special-casing here.
// Adding a new element type only requires REGISTER_PIPELINE_ELEMENT in its .cpp file.
class YAMLPipeline {
public:
    // Parses the config file and returns a ready-to-build Pipeline (or subclass).
    // Throws std::runtime_error on parse errors or unknown element types.
    static std::unique_ptr<Pipeline> fromFile(const std::string& path);

private:
    static std::shared_ptr<PipelineElement> buildElement(const YAML::Node& node);
    static std::vector<std::shared_ptr<PipelineElement>> buildElementList(const YAML::Node& list);
};

} // namespace camera_driver
