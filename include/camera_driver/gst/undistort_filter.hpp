#pragma once

#include "../metadata/camera_metadata.hpp"
#include <gst/gst.h>

namespace camera_driver {

// Name of the registered GStreamer element type — reference this in
// gst_parse_launch strings (e.g. "camera_driver_undistort name=foo").
// A real GstVideoFilter subclass (not an appsink/appsrc bridge): plain C++
// pinhole radial/tangential undistort, no OpenCV (see UndistortElement's
// header for why OpenCV is off-limits in this process).
inline constexpr const char* kUndistortFilterElementName = "camera_driver_undistort";

// Registers the camera_driver_undistort element type with GStreamer's
// registry. Idempotent. Must happen before any gst_parse_launch() call that
// references the element by name.
void registerUndistortFilter();

// Sets the calibration `filter` (an instance of camera_driver_undistort,
// e.g. looked up via Pipeline::getGstElement() after build()) undistorts
// with. The internal remap table is (re)built lazily on the next frame if
// the calibration or negotiated frame size changed.
void setUndistortFilterCalibration(GstElement* filter, const CalibrationInfo& calib);

} // namespace camera_driver
