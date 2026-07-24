#pragma once

#include <array>
#include <optional>
#include <string>
#include <vector>

namespace camera_driver {

// Mirrors the ROS `sensor_msgs/CameraInfo` field set and the YAML schema used
// by ROS `camera_info_manager` calibration files 1:1, so existing ROS
// calibration YAML can be loaded directly via CameraMetadata::fromYaml().
struct CalibrationInfo {
    int width  = 0;
    int height = 0;
    std::string distortion_model;
    std::vector<double>  D;       // distortion coefficients
    std::array<double, 9>  K{};   // camera matrix (row-major 3x3)
    std::array<double, 9>  R{};   // rectification matrix (row-major 3x3)
    std::array<double, 12> P{};   // projection matrix (row-major 3x4)
};

// Camera pose at time of recording (extrinsics), not a per-frame synced track.
struct PoseInfo {
    std::string frame_id;
    double px = 0, py = 0, pz = 0;             // position
    double qx = 0, qy = 0, qz = 0, qw = 1;      // orientation quaternion
};

// Calibration + pose metadata carried alongside a recorded video/image.
// Serialized to/from YAML and embedded as a single GST_TAG_COMMENT string
// (see MkvRecorderElement / metadata_io) prefixed with "camera-metadata=".
struct CameraMetadata {
    std::optional<CalibrationInfo> calibration;
    std::optional<PoseInfo> pose;

    std::string toYaml() const;
    static CameraMetadata fromYaml(const std::string& yaml);

    // Loads only the calibration block, using the ROS camera_info YAML
    // schema (camera_matrix/distortion_model/distortion_coefficients/...).
    // Accepts both a bare camera_info document and one nested under a
    // top-level `calibration:` key (as produced by toYaml()).
    static CalibrationInfo calibrationFromYamlFile(const std::string& path);
};

} // namespace camera_driver
