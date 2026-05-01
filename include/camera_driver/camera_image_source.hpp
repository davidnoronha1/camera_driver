#pragma once
#include <utils/image_source.hpp>
#include <string>

namespace camera_driver {

/**
 * Extended ImageSource for the camera_driver node.
 *
 * Adds usb:// support (physical V4L2/USB cameras) on top of the base
 * utils::ImageSource schemes, and provides helpers for building GStreamer
 * pipeline source fragments.
 *
 * Supported URIs:
 *   file:///path/to/video.mp4   local video file  → filesrc + decodebin
 *   usb://                      auto-discover USB camera (use node params)
 *   usb:///dev/videoN           specific V4L2 device path
 */
class CameraImageSource : public utils::ImageSource
{
public:
    explicit CameraImageSource(const std::string & uri)
    : utils::ImageSource(uri) {}

    /// True when this source maps to a physical camera (USB or unrecognised URI).
    bool is_physical_camera() const
    {
        return is_usb() || scheme() == utils::ImageScheme::UNKNOWN || uri().empty();
    }

    /**
     * GStreamer filesrc fragment for file:// sources, ready to pipe into
     * decodebin.  Returns an empty string for non-file sources.
     *
     * Example: "filesrc location=/path/to/video.mp4"
     */
    std::string gst_file_src() const
    {
        if (!is_file()) return "";
        return "filesrc location=" + path();
    }

    /**
     * Explicit V4L2 device path encoded in the URI (e.g. usb:///dev/video0).
     * Returns an empty string when the usb:// URI carries no device path, in
     * which case the caller should perform device discovery via the node params.
     */
    std::string usb_device_path() const
    {
        if (!is_usb()) return "";
        const std::string p = path();  // strips "usb://"
        return (p.size() > 0 && p[0] == '/') ? p : "";
    }
};

}  // namespace camera_driver
