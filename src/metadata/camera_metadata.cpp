#include "camera_driver/metadata/camera_metadata.hpp"
#include <yaml-cpp/yaml.h>

namespace camera_driver {

namespace {

YAML::Node calibrationToNode(const CalibrationInfo& c) {
    YAML::Node n;
    n["image_width"]      = c.width;
    n["image_height"]     = c.height;
    n["distortion_model"] = c.distortion_model;

    YAML::Node k; k["rows"] = 3; k["cols"] = 3;
    for (double v : c.K) k["data"].push_back(v);
    n["camera_matrix"] = k;

    YAML::Node d; d["rows"] = 1; d["cols"] = static_cast<int>(c.D.size());
    for (double v : c.D) d["data"].push_back(v);
    n["distortion_coefficients"] = d;

    YAML::Node r; r["rows"] = 3; r["cols"] = 3;
    for (double v : c.R) r["data"].push_back(v);
    n["rectification_matrix"] = r;

    YAML::Node p; p["rows"] = 3; p["cols"] = 4;
    for (double v : c.P) p["data"].push_back(v);
    n["projection_matrix"] = p;

    return n;
}

template <size_t N>
void loadFixedArray(const YAML::Node& matrix_node, std::array<double, N>& out) {
    if (!matrix_node || !matrix_node["data"]) return;
    const YAML::Node data = matrix_node["data"];
    for (size_t i = 0; i < N && i < data.size(); ++i) out[i] = data[i].as<double>();
}

// Accepts both the ROS camera_info_manager schema (image_width/camera_matrix/...)
// this reads, and is what CameraMetadata::toYaml() also writes for round-trip
// interop with real ROS calibration files.
CalibrationInfo calibrationFromNode(const YAML::Node& n) {
    CalibrationInfo c;
    c.width            = n["image_width"].as<int>(0);
    c.height           = n["image_height"].as<int>(0);
    c.distortion_model = n["distortion_model"].as<std::string>("");

    loadFixedArray(n["camera_matrix"], c.K);
    loadFixedArray(n["rectification_matrix"], c.R);
    loadFixedArray(n["projection_matrix"], c.P);

    if (n["distortion_coefficients"] && n["distortion_coefficients"]["data"]) {
        for (const auto& v : n["distortion_coefficients"]["data"])
            c.D.push_back(v.as<double>());
    }
    return c;
}

YAML::Node poseToNode(const PoseInfo& p) {
    YAML::Node n;
    n["frame_id"] = p.frame_id;
    YAML::Node pos; pos["x"] = p.px; pos["y"] = p.py; pos["z"] = p.pz;
    n["position"] = pos;
    YAML::Node ori; ori["x"] = p.qx; ori["y"] = p.qy; ori["z"] = p.qz; ori["w"] = p.qw;
    n["orientation"] = ori;
    return n;
}

PoseInfo poseFromNode(const YAML::Node& n) {
    PoseInfo p;
    p.frame_id = n["frame_id"].as<std::string>("");
    if (n["position"]) {
        p.px = n["position"]["x"].as<double>(0);
        p.py = n["position"]["y"].as<double>(0);
        p.pz = n["position"]["z"].as<double>(0);
    }
    if (n["orientation"]) {
        p.qx = n["orientation"]["x"].as<double>(0);
        p.qy = n["orientation"]["y"].as<double>(0);
        p.qz = n["orientation"]["z"].as<double>(0);
        p.qw = n["orientation"]["w"].as<double>(1);
    }
    return p;
}

} // namespace

std::string CameraMetadata::toYaml() const {
    YAML::Node root;
    if (calibration) root["calibration"] = calibrationToNode(*calibration);
    if (pose)        root["pose"]        = poseToNode(*pose);
    YAML::Emitter emitter;
    emitter << root;
    return std::string(emitter.c_str());
}

CameraMetadata CameraMetadata::fromYaml(const std::string& yaml) {
    CameraMetadata m;
    YAML::Node root = YAML::Load(yaml);
    if (root["calibration"]) m.calibration = calibrationFromNode(root["calibration"]);
    if (root["pose"])        m.pose        = poseFromNode(root["pose"]);
    return m;
}

CalibrationInfo CameraMetadata::calibrationFromYamlFile(const std::string& path) {
    YAML::Node root = YAML::LoadFile(path);
    const YAML::Node node = root["calibration"] ? root["calibration"] : root;
    return calibrationFromNode(node);
}

} // namespace camera_driver
