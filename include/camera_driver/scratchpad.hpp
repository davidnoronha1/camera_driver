#pragma once

#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace camera_driver {

// Generic, thread-safe key -> string-blob store shared by every element in a
// Pipeline (see Pipeline::scratchpad()). Not camera-specific — any element
// can stash/retrieve arbitrary data here (calibration, pose, or anything
// else added later) without the pipeline's owner having to manually wire it
// between elements. Values are opaque strings; callers agree on their own
// encoding per key. This project uses YAML for structured values (see
// CameraMetadata::toYaml()) under kCameraMetadataKey.
//
// In-process only — see ScratchpadServer/ScratchpadClient (scratchpad_server.hpp)
// for exposing the same data to other processes over a Unix socket.
class Scratchpad {
public:
    static constexpr const char* kCameraMetadataKey = "camera-metadata";

    void set(const std::string& key, std::string value);
    std::optional<std::string> get(const std::string& key) const;
    bool erase(const std::string& key);

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::string> values_;
};

} // namespace camera_driver
