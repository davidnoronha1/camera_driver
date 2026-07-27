#include "camera_driver/pipelines/yaml_pipeline.hpp"
#include "camera_driver/element_registry.hpp"
#include "camera_driver/lflogger.hpp"
#include "camera_driver/metadata/camera_metadata.hpp"

#ifdef CAMERA_DRIVER_WITH_RTSP
#include "camera_driver/pipelines/rtsp_pipeline.hpp"
#endif

#include <fmt/format.h>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace camera_driver {

std::unique_ptr<Pipeline> YAMLPipeline::fromFile(const std::string& path) {
    auto& log = LockFreeLogger::getInstance();
    log.info("yaml_pipeline", "Loading pipeline from: " + path);

    YAML::Node root;
    try {
        root = YAML::LoadFile(path);
    } catch (const std::exception& e) {
        throw std::runtime_error("YAMLPipeline: cannot parse '" + path + "': " + e.what());
    }

    // Determine pipeline type
    std::string type = "Pipeline";
    if (root["pipeline"] && root["pipeline"]["type"])
        type = root["pipeline"]["type"].as<std::string>();

    std::unique_ptr<Pipeline> pipeline;

#ifdef CAMERA_DRIVER_WITH_RTSP
    if (type == "RTSPPipeline") {
        int port         = root["pipeline"]["port"].as<int>(8554);
        std::string mnt  = root["pipeline"]["mount_point"].as<std::string>("/stream");
        pipeline = std::make_unique<RTSPPipeline>(port, mnt);
    } else
#else
    if (type == "RTSPPipeline") {
        throw std::runtime_error("RTSPPipeline was requested, but RTSP support was not compiled. "
                                 "Please install libgstrtspserver-1.0-dev and rebuild the project.");
    } else
#endif
    {
        pipeline = std::make_unique<Pipeline>();
    }

    // Calibration/pose metadata, if provided, is the single source every
    // sink pulls from via Pipeline::scratchpad() (Scratchpad::kCameraMetadataKey)
    // — no per-element metadata_file/calibration_file wiring needed.
    if (root["pipeline"] && (root["pipeline"]["metadata_file"] || root["pipeline"]["calibration_file"])) {
        CameraMetadata metadata;
        if (root["pipeline"]["metadata_file"]) {
            std::ifstream in(root["pipeline"]["metadata_file"].as<std::string>());
            std::stringstream ss;
            ss << in.rdbuf();
            metadata = CameraMetadata::fromYaml(ss.str());
        } else {
            metadata.calibration = CameraMetadata::calibrationFromYamlFile(
                root["pipeline"]["calibration_file"].as<std::string>());
        }
        pipeline->scratchpad()->set(Scratchpad::kCameraMetadataKey, metadata.toYaml());
        log.info("yaml_pipeline", "Loaded camera metadata into pipeline scratchpad");
    }

    // Build element list
    if (!root["elements"] || !root["elements"].IsSequence())
        throw std::runtime_error("YAMLPipeline: 'elements' list is missing or not a sequence");

    for (const auto& el_node : root["elements"]) {
        if (!el_node["type"])
            throw std::runtime_error("YAMLPipeline: element missing 'type' field");

        std::string el_type = el_node["type"].as<std::string>();
        try {
            auto el = ElementRegistry::instance().create(el_type, el_node);
            pipeline->add(el);
            log.info("yaml_pipeline", "Added element: " + el_type);
        } catch (const std::exception& e) {
            throw std::runtime_error(fmt::format(
                "YAMLPipeline: failed to create element '{}': {}", el_type, e.what()));
        }
    }

    return pipeline;
}

std::shared_ptr<PipelineElement> YAMLPipeline::buildElement(const YAML::Node& node) {
    std::string type = node["type"].as<std::string>();
    return ElementRegistry::instance().create(type, node);
}

std::vector<std::shared_ptr<PipelineElement>> YAMLPipeline::buildElementList(const YAML::Node& list) {
    std::vector<std::shared_ptr<PipelineElement>> elements;
    for (const auto& node : list)
        elements.push_back(buildElement(node));
    return elements;
}

} // namespace camera_driver
