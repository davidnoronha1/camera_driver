// camera_rtsp_streamer_v7.cpp
//
// Single-pipeline architecture with multi-source support.
//
// `image_source` parameter selects the input:
//
//   file://<abs-path>        – Video file, looped via OpenCV VideoCapture.
//                              An OpenCV thread reads frames and pushes them
//                              into a GStreamer appsrc element; when the file
//                              ends VideoCapture::set(CAP_PROP_POS_FRAMES, 0)
//                              resets it to frame 0 and playback continues
//                              without interruption.
//
//   rtsp://<url>             – Re-stream an existing RTSP source.
//
//   usb://<bus_path>         – USB camera matched by GstDeviceMonitor
//                              device.bus_path (e.g. usb://1-1.2).
//
//   device://<index>         – V4L2 device by number
//                              (device://0  →  /dev/video0).
//
//   (empty)                  – Legacy behaviour: device_path / vendor_id /
//                              product_id / serial_no / usb_port parameters.
//
// ── Pipeline shapes ────────────────────────────────────────────────────────
//
//  file:// source
//   [OpenCV thread] → appsrc (RGB) → videoconvert → [crop] → tee
//       ├─ queue → videoconvert → I420 → x264enc → rtph264pay   (RTSP)
//       └─ queue → videoconvert → RGB  → appsink                (ROS 2)
//
//  rtsp:// source
//   rtspsrc → decodebin → videoconvert → [crop] → tee  (same tee branches)
//
//  USB / device  –  MJPEG or raw
//   v4l2src → [jpegdec] → videoconvert → [crop] → tee  (same tee branches)
//
//  USB / device  –  H.264 passthrough
//   v4l2src → h264parse → tee
//       ├─ queue → rtph264pay                                    (RTSP)
//       └─ queue → avdec_h264 → videoconvert → [crop] → RGB → appsink (ROS 2)

#include <atomic>
#include <future>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>

#include <opencv2/videoio.hpp>
#include <opencv2/imgproc.hpp>

#include <gst/app/gstappsink.h>
#include <gst/app/gstappsrc.h>
#include <gst/gst.h>
#include <gst/gstdevice.h>
#include <gst/gstdevicemonitor.h>
#include <gst/gststructure.h>
#include <gst/rtsp-server/rtsp-server.h>
#include <gst/rtsp-server/rtsp-server-object.h>

#include <camera_info_manager/camera_info_manager.hpp>
#include <image_transport/image_transport.hpp>
#include <rclcpp/logging.hpp>
#include <rclcpp/rclcpp.hpp>
#include <sensor_msgs/msg/camera_info.hpp>
#include <sensor_msgs/msg/image.hpp>

// ── Helpers ────────────────────────────────────────────────────────────────

static const char *get_machine_ip() {
  const char *x = getenv("MACHINE_IP");
  return x ? x : "{MACHINE_IP}";
}

// ── Source type ────────────────────────────────────────────────────────────

enum class SourceType { File, RTSP, USB, Device, Legacy };

struct ParsedSource {
  SourceType  type = SourceType::Legacy;
  std::string value; // file path, full rtsp URL, usb bus_path, or device index
};

static ParsedSource parse_image_source(const std::string &raw) {
  if (raw.empty()) return {SourceType::Legacy, ""};

  auto sep = raw.find("://");
  if (sep == std::string::npos) return {SourceType::Legacy, raw};

  std::string scheme = raw.substr(0, sep);
  std::string rest   = raw.substr(sep + 3);
  for (auto &c : scheme) c = static_cast<char>(tolower(static_cast<unsigned char>(c)));

  if (scheme == "file")   return {SourceType::File,   rest};
  if (scheme == "rtsp")   return {SourceType::RTSP,   raw};   // keep full URL
  if (scheme == "usb")    return {SourceType::USB,    rest};
  if (scheme == "device") return {SourceType::Device, rest};

  return {SourceType::Legacy, raw};
}

// ══════════════════════════════════════════════════════════════════════════════
class RTSPCameraStreamer : public rclcpp::Node {
public:
  RTSPCameraStreamer() : Node("rtsp_camera_streamer") {}

  // Called after make_shared() so shared_from_this() is valid.
  void initialize() {
    gst_init(nullptr, nullptr);
    declare_and_read_parameters();
    setup_ros_publishers();

    // Build the GStreamer pipeline string (or register file-source machinery).
    std::string pipeline_str = build_pipeline();
    RCLCPP_INFO(get_logger(), "GStreamer pipeline:\n  %s", pipeline_str.c_str());
    log_configuration(pipeline_str);

    // ── RTSP server ────────────────────────────────────────────────────────
    server_ = gst_rtsp_server_new();
    gst_rtsp_server_set_service(server_, std::to_string(port_).c_str());

    GstRTSPMountPoints  *mounts  = gst_rtsp_server_get_mount_points(server_);
    GstRTSPMediaFactory *factory = gst_rtsp_media_factory_new();
    gst_rtsp_media_factory_set_launch(factory, pipeline_str.c_str());
    gst_rtsp_media_factory_set_shared(factory, TRUE);

    g_signal_connect(factory, "media-configure",
                     G_CALLBACK(on_media_configure_static), this);

    gst_rtsp_mount_points_add_factory(mounts, mount_point_, factory);
    g_object_unref(mounts);

    // ── GLib main loop (owns the RTSP server) ──────────────────────────────
    glib_context_ = g_main_context_new();
    loop_         = g_main_loop_new(glib_context_, FALSE);

    std::promise<void> ready;
    auto ready_fut = ready.get_future();
    glib_thread_   = std::thread([this, p = std::move(ready)]() mutable {
      GSource *idle = g_idle_source_new();
      g_source_set_callback(idle,
                            [](gpointer d) -> gboolean {
                              static_cast<std::promise<void> *>(d)->set_value();
                              return G_SOURCE_REMOVE;
                            },
                            &p, nullptr);
      g_source_attach(idle, glib_context_);
      g_source_unref(idle);
      g_main_loop_run(loop_);
    });
    ready_fut.wait();

    if (gst_rtsp_server_attach(server_, glib_context_) == 0)
      throw std::runtime_error("Failed to attach RTSP server to GLib context");

    RCLCPP_INFO(get_logger(), "RTSP server ready — rtsp://%s:%ld%s",
                get_machine_ip(), port_, mount_point_);
  }

  ~RTSPCameraStreamer() { cleanup(); }

  // ── Encoder ───────────────────────────────────────────────────────────────
  enum class EncoderType { X264, QSV, NVENC, V4L2 };

  struct EncoderInfo {
    EncoderType type;
    std::string name;
    std::string extra_props;
  };

  static std::string caps_for_encoder(EncoderType t) {
    switch (t) {
    case EncoderType::QSV:
    case EncoderType::NVENC:
    case EncoderType::V4L2:  return "video/x-raw,format=NV12";
    case EncoderType::X264:
    default:                  return "video/x-raw,format=I420";
    }
  }

  static bool gst_element_exists(const std::string &name) {
    GstElementFactory *f = gst_element_factory_find(name.c_str());
    if (!f) return false;
    gst_object_unref(f);
    return true;
  }

  EncoderInfo detect_best_encoder() const {
    long br = bitrate_, fr = framerate_;
    if (gst_element_exists("qsvh264enc")) {
      RCLCPP_INFO(get_logger(), "Encoder: Intel QSV");
      return {EncoderType::QSV, "qsvh264enc",
              " bitrate=" + std::to_string(br) +
              " target-usage=7 gop-size=" + std::to_string(fr) +
              " b-frames=0 low-latency=true rate-control=cbr"};
    }
    if (gst_element_exists("nvh264enc")) {
      RCLCPP_INFO(get_logger(), "Encoder: NVIDIA NVENC");
      return {EncoderType::NVENC, "nvh264enc",
              " bitrate=" + std::to_string(br) +
              " preset=low-latency-hp gop-size=" + std::to_string(fr) +
              " bframes=0 rc-mode=cbr zerolatency=true"};
    }
    if (gst_element_exists("v4l2h264enc")) {
      RCLCPP_INFO(get_logger(), "Encoder: V4L2 M2M hardware");
      return {EncoderType::V4L2, "v4l2h264enc",
              " extra-controls=\"encode,video_bitrate=" +
              std::to_string(br * 1000) + "\""};
    }
    RCLCPP_WARN(get_logger(), "Encoder: software x264 (no hardware encoder)");
    return {EncoderType::X264, "x264enc",
            " bitrate=" + std::to_string(br) +
            " speed-preset=ultrafast tune=zerolatency key-int-max=" +
            std::to_string(fr) +
            " bframes=0 sliced-threads=true rc-lookahead=0"
            " sync-lookahead=0 vbv-buf-capacity=0"};
  }

private:
  // ── Parameter declaration & reading ───────────────────────────────────────
  void declare_and_read_parameters() {
    declare_parameter("image_source",   "");       // NEW unified source param
    // Legacy V4L2 / USB params (used when image_source is empty)
    declare_parameter("vendor_id",      -1);
    declare_parameter("product_id",     -1);
    declare_parameter("serial_no",      "");
    declare_parameter("device_path",    "");
    declare_parameter("usb_port",       "");
    // Common params
    declare_parameter("image_width",    640);
    declare_parameter("image_height",   480);
    declare_parameter("frame_format",   "MJPEG");
    declare_parameter("framerate",      30);
    declare_parameter("port",           8554);
    declare_parameter("bitrate",        2000);
    declare_parameter("ros_topic",      "");
    declare_parameter("camera_frame_id","camera");
    declare_parameter("camera_info_url","");
    // Fractional crop "x1,y1,x2,y2" in [0,1]
    declare_parameter("crop",           "");

    source_       = parse_image_source(get_parameter("image_source").as_string());
    width_        = get_parameter("image_width").as_int();
    height_       = get_parameter("image_height").as_int();
    fmt_          = get_parameter("frame_format").as_string();
    framerate_    = get_parameter("framerate").as_int();
    port_         = get_parameter("port").as_int();
    bitrate_      = get_parameter("bitrate").as_int();
    frame_id_     = get_parameter("camera_frame_id").as_string();
    crop_param_   = get_parameter("crop").as_string();

    ros_topic_basename_ = get_parameter("ros_topic").as_string();
    if (ros_topic_basename_.empty()) ros_topic_basename_ = frame_id_;

    std::string camera_info_url = get_parameter("camera_info_url").as_string();
    camera_info_manager_ = std::make_shared<camera_info_manager::CameraInfoManager>(
        this, frame_id_);
    if (!camera_info_url.empty()) {
      if (camera_info_manager_->validateURL(camera_info_url))
        camera_info_manager_->loadCameraInfo(camera_info_url);
      else
        RCLCPP_WARN(get_logger(), "camera_info_url '%s' invalid — uncalibrated",
                    camera_info_url.c_str());
    }
  }

  void setup_ros_publishers() {
    image_transport::ImageTransport it(shared_from_this());
    image_pub_ = it.advertise(ros_topic_basename_ + "/image", 1);
    camera_info_pub_ = create_publisher<sensor_msgs::msg::CameraInfo>(
        ros_topic_basename_ + "/camera_info", rclcpp::SensorDataQoS());
  }

  // ── Crop helper ───────────────────────────────────────────────────────────
  // Returns a "videocrop ... ! " string or "" if crop_param is empty/invalid.
std::string build_videocrop(long w, long h) const {
  if (crop_param_.empty()) return "";

  double x1 = 0, y1 = 0, x2 = 1, y2 = 1;
  if (sscanf(crop_param_.c_str(), "%lf,%lf,%lf,%lf", &x1, &y1, &x2, &y2) != 4) {
    RCLCPP_WARN(get_logger(), "crop '%s' not in x1,y1,x2,y2 format — ignored",
                crop_param_.c_str());
    return "";
  }

  // Validate fractions are in [0,1] and x1 < x2, y1 < y2
  if (x1 < 0.0 || x2 > 1.0 || y1 < 0.0 || y2 > 1.0 ||
      x1 >= x2 || y1 >= y2) {
    RCLCPP_ERROR(get_logger(),
                 "crop '%s' invalid: values must be in [0,1] with x1<x2 and y1<y2 — ignored",
                 crop_param_.c_str());
    return "";
  }

  int left   = static_cast<int>(x1 * w);
  int top    = static_cast<int>(y1 * h);
  int right  = static_cast<int>((1.0 - x2) * w);
  int bottom = static_cast<int>((1.0 - y2) * h);

  // Guard against degenerate crop (output must be at least 2x2)
  long out_w = w - left - right;
  long out_h = h - top - bottom;
  if (out_w < 2 || out_h < 2) {
    RCLCPP_ERROR(get_logger(),
                 "crop '%s' produces degenerate output %ldx%ld from %ldx%ld — ignored",
                 crop_param_.c_str(), out_w, out_h, w, h);
    return "";
  }

  RCLCPP_INFO(get_logger(),
              "Crop: left=%d right=%d top=%d bottom=%d (%ldx%ld → %ldx%ld)",
              left, right, top, bottom,
              w, h, out_w, out_h);

  return "videocrop left=" + std::to_string(left) +
         " right="  + std::to_string(right) +
         " top="    + std::to_string(top) +
         " bottom=" + std::to_string(bottom) + " ! ";
}

  // ── Common tee + encode + appsink tail ────────────────────────────────────
  // Appended after whatever source + optional crop lands us at raw RGB/YUV.
  // `after_tee_convert` — if true, inserts a videoconvert before the tee
  // (needed when the upstream element doesn't guarantee a negotiable format).
  std::string build_tee_tail(bool after_tee_convert = true) const {
    auto enc = detect_best_encoder();
    std::ostringstream ss;
    if (after_tee_convert) ss << "videoconvert ! ";
    ss << "tee name=t "
       // Branch 1 — RTSP / H.264
       << "t. ! queue leaky=downstream max-size-buffers=2 ! "
       << "videoconvert ! " << caps_for_encoder(enc.type) << " ! "
       << enc.name << enc.extra_props << " ! "
       << "rtph264pay name=pay0 pt=96 config-interval=1 "
       // Branch 2 — ROS 2 appsink
       << "t. ! queue leaky=downstream max-size-buffers=2 ! "
       << "videoconvert ! video/x-raw,format=RGB ! "
       << "appsink name=ros_sink max-buffers=2 drop=true sync=false ";
    return ss.str();
  }

  // ── Pipeline builder (dispatcher) ─────────────────────────────────────────
  std::string build_pipeline() {
    switch (source_.type) {
    case SourceType::File:   return build_pipeline_file();
    case SourceType::RTSP:   return build_pipeline_rtsp();
    case SourceType::USB:    return build_pipeline_usb_by_bus(source_.value);
    case SourceType::Device: return build_pipeline_device(
                                 "/dev/video" + source_.value);
    case SourceType::Legacy: return build_pipeline_legacy();
    }
    return build_pipeline_legacy();
  }

  // ── file:// — OpenCV feeds an appsrc ─────────────────────────────────────
  //
  // We open the file here only to probe width/height/fps so we can set the
  // appsrc caps correctly.  The actual read loop is started in
  // on_media_configure() once the pipeline is live.
 std::string build_pipeline_file() {
  RCLCPP_INFO(get_logger(), "Source: file (OpenCV loop) — %s",
              source_.value.c_str());

  // Probe the file for dimensions and frame rate.
  cv::VideoCapture probe(source_.value);
  if (!probe.isOpened())
    throw std::runtime_error("Cannot open file: " + source_.value);

  long w  = static_cast<long>(probe.get(cv::CAP_PROP_FRAME_WIDTH));
  long h  = static_cast<long>(probe.get(cv::CAP_PROP_FRAME_HEIGHT));
  double fps = probe.get(cv::CAP_PROP_FPS);
  probe.release();

  if (w > 0)   width_     = w;
  if (h > 0)   height_    = h;
  if (fps > 0) framerate_ = static_cast<long>(fps);

  RCLCPP_INFO(get_logger(), "File probe: %ldx%ld @ %.2f fps",
              width_, height_, static_cast<double>(framerate_));

  std::string crop = build_videocrop(width_, height_);
  std::ostringstream ss;
  // appsrc pushes RGB frames (converted in file_loop_thread).
  // Declaring RGB avoids a BGR->anything negotiation dead-end.
  // videocrop sits immediately after appsrc (both are RGB) so the
  // tee branches only ever see a single unambiguous format.
  ss << "( appsrc name=file_src"
     << " caps=video/x-raw,format=RGB"
     << ",width="     << width_
     << ",height="    << height_
     << ",framerate=" << framerate_ << "/1"
     << " format=time is-live=true do-timestamp=true block=true ! "
     << crop                                        // videocrop here, already RGB
     << build_tee_tail(/*after_tee_convert=*/true)  // videoconvert inside each branch
     << ")";
  return ss.str();
}

  // ── rtsp:// — rtspsrc → decodebin ────────────────────────────────────────
  std::string build_pipeline_rtsp() {
    RCLCPP_INFO(get_logger(), "Source: RTSP — %s", source_.value.c_str());
    std::string crop = build_videocrop(width_, height_);
    std::ostringstream ss;
    ss << "( rtspsrc location=" << source_.value
       << " latency=100 ! decodebin ! "
       << "videoconvert ! "
       << crop
       << build_tee_tail(false)
       << ")";
    return ss.str();
  }

  // ── usb://<bus_path> ──────────────────────────────────────────────────────
  std::string build_pipeline_usb_by_bus(const std::string &bus_path) {
    std::string device_path;
    GstDevice *dev = find_camera_by_usb_port(bus_path, device_path);
    if (!dev)
      throw std::runtime_error("No camera found at USB bus path: " + bus_path);

    negotiate_capabilities(dev);
    gst_object_unref(dev);

    RCLCPP_INFO(get_logger(), "Source: USB bus_path=%s → %s",
                bus_path.c_str(), device_path.c_str());
    return build_pipeline_v4l2(device_path);
  }

  // ── device://<index> — /dev/video<N> ─────────────────────────────────────
  std::string build_pipeline_device(const std::string &dev_path) {
    RCLCPP_INFO(get_logger(), "Source: device — %s", dev_path.c_str());
    GstDevice *dev = find_camera_by_path(dev_path);
    if (dev) {
      negotiate_capabilities(dev);
      gst_object_unref(dev);
    }
    return build_pipeline_v4l2(dev_path);
  }

  // ── Legacy — original vendor/product/serial/device_path/usb_port logic ───
  std::string build_pipeline_legacy() {
    std::string usb_port   = get_parameter("usb_port").as_string();
    std::string device_path = get_parameter("device_path").as_string();
    long vendor  = get_parameter("vendor_id").as_int();
    long product = get_parameter("product_id").as_int();
    std::string serial = get_parameter("serial_no").as_string();

    GstDevice *dev = nullptr;

    if (!usb_port.empty())
      dev = find_camera_by_usb_port(usb_port, device_path);

    if (!dev)
      dev = find_camera_by_id(vendor, product, serial);

    if (!dev && !device_path.empty())
      dev = find_camera_by_path(device_path);

    if (!dev)
      dev = find_camera_any(device_path);

    if (!dev)
      throw std::runtime_error("No camera found (legacy discovery failed)");

    RCLCPP_INFO(get_logger(), "Using camera: %s",
                gst_device_get_display_name(dev));

    negotiate_capabilities(dev);
    gst_object_unref(dev);

    return build_pipeline_v4l2(device_path);
  }

  // ── V4L2 pipeline (USB / device / legacy end-path) ────────────────────────
  std::string build_pipeline_v4l2(const std::string &device_path) {
    std::string gst_fmt = gst_caps_for_format(fmt_);
    std::string crop    = build_videocrop(width_, height_);
    std::ostringstream ss;

    if (strcasecmp(fmt_.c_str(), "H264") == 0) {
      RCLCPP_INFO(get_logger(), "Pipeline mode: H.264 passthrough");
      ss << "( v4l2src device=" << device_path << " ! "
         << gst_fmt << ",width=" << width_ << ",height=" << height_
         << ",framerate=" << framerate_ << "/1 ! "
         << "h264parse ! tee name=t "
         // RTSP branch
         << "t. ! queue leaky=downstream max-size-buffers=2 ! "
         << "rtph264pay name=pay0 pt=96 config-interval=1 "
         // ROS 2 branch — decode then crop
         << "t. ! queue leaky=downstream max-size-buffers=2 ! "
         << "avdec_h264 ! videoconvert ! "
         << crop
         << "video/x-raw,format=RGB ! "
         << "appsink name=ros_sink max-buffers=2 drop=true sync=false "
         << ")";
    } else {
      RCLCPP_INFO(get_logger(), "Pipeline mode: %s → encode", fmt_.c_str());
      auto enc = detect_best_encoder();
      ss << "( v4l2src device=" << device_path << " ! "
         << gst_fmt << ",width=" << width_ << ",height=" << height_
         << ",framerate=" << framerate_ << "/1 ! ";

      if (strcasecmp(fmt_.c_str(), "MJPEG") == 0)
        ss << "jpegdec ! ";

      ss << "videoconvert ! "
         << crop
         << build_tee_tail(false)
         << ")";
    }
    return ss.str();
  }

  // ── Capability negotiation (V4L2 sources) ─────────────────────────────────
  void negotiate_capabilities(GstDevice *dev) {
    std::set<std::string> formats;
    std::set<std::pair<int,int>> resolutions;
    query_camera_capabilities(dev, formats, resolutions);

    if (!formats.empty()) {
      bool found = false;
      for (auto &sf : formats)
        if (strcasecmp(sf.c_str(), fmt_.c_str()) == 0) { found = true; break; }
      if (!found) {
        if (formats.count("H264"))       fmt_ = "H264";
        else if (formats.count("MJPEG")) fmt_ = "MJPEG";
        else                             fmt_ = *formats.begin();
        RCLCPP_WARN(get_logger(), "Format not supported, using '%s'", fmt_.c_str());
      }
    }

    if (!resolutions.empty()) {
      bool found = false;
      for (auto &r : resolutions)
        if (r.first == width_ && r.second == height_) { found = true; break; }
      if (!found) {
        auto best = *resolutions.begin();
        int target = width_ * height_;
        int min_d  = std::abs(best.first * best.second - target);
        for (auto &r : resolutions) {
          int d = std::abs(r.first * r.second - target);
          if (d < min_d) { min_d = d; best = r; }
        }
        RCLCPP_WARN(get_logger(), "Resolution %ldx%ld unsupported, using %dx%d",
                    width_, height_, best.first, best.second);
        width_  = best.first;
        height_ = best.second;
      }
    }
  }

  // ── media-configure signal ─────────────────────────────────────────────────
  // Called by the RTSP server when a client connects and the pipeline is ready.
  // We fish out ros_sink (appsink) and — for file sources — file_src (appsrc).
  static void on_media_configure_static(GstRTSPMediaFactory *,
                                        GstRTSPMedia *media,
                                        gpointer user_data) {
    static_cast<RTSPCameraStreamer *>(user_data)->on_media_configure(media);
  }

 void on_media_configure(GstRTSPMedia *media) {
  GstElement *bin = gst_rtsp_media_get_element(media);
  if (!bin) { RCLCPP_ERROR(get_logger(), "media-configure: no pipeline"); return; }

  // ── Wire up ROS 2 appsink ─────────────────────────────────────────────
  GstElement *appsink = gst_bin_get_by_name(GST_BIN(bin), "ros_sink");
  if (!appsink) {
    RCLCPP_ERROR(get_logger(), "media-configure: ros_sink not found");
    gst_object_unref(bin);
    return;
  }
  GstAppSinkCallbacks cbs{};
  cbs.new_sample = &RTSPCameraStreamer::on_new_sample_static;
  gst_app_sink_set_callbacks(GST_APP_SINK(appsink), &cbs, this, nullptr);
  gst_object_unref(appsink);

  // ── Start/restart OpenCV file-loop thread if this is a file source ────
  if (source_.type == SourceType::File) {
    GstElement *appsrc = gst_bin_get_by_name(GST_BIN(bin), "file_src");
    if (!appsrc) {
      RCLCPP_ERROR(get_logger(), "media-configure: file_src appsrc not found");
      gst_object_unref(bin);
      return;
    }

    // Always stop any existing thread before starting a new one
    stop_file_thread();

    file_appsrc_  = GST_APP_SRC(gst_object_ref(appsrc));
    file_running_ = true;
    file_thread_  = std::thread(&RTSPCameraStreamer::file_loop_thread, this);

    gst_object_unref(appsrc);
    RCLCPP_INFO(get_logger(), "File source thread (re)started for: %s",
                source_.value.c_str());
  }

  gst_object_unref(bin);
  RCLCPP_INFO(get_logger(), "RTSP client connected — ROS 2 publishing active");
}

  // ── OpenCV file-loop thread ───────────────────────────────────────────────
  //
  // Reads frames from the video file using cv::VideoCapture, converts each
  // frame to BGR (GStreamer appsrc caps), wraps it in a GstBuffer, and pushes
  // it into the pipeline.  When the file ends (read returns false),
  // CAP_PROP_POS_FRAMES is reset to 0 and reading continues — seamless loop.
  //
  // Frame timing is enforced by sleeping for one frame period between pushes
  // so the downstream encoder and RTSP payloader see a steady rate.
void file_loop_thread() {
  cv::VideoCapture cap(source_.value);
  if (!cap.isOpened()) {
    RCLCPP_ERROR(get_logger(), "File loop: cannot open '%s'",
                 source_.value.c_str());
    return;
  }

  const long frame_period_us =
      static_cast<long>(1e6 / static_cast<double>(framerate_));
  const std::size_t frame_bytes =
      static_cast<std::size_t>(width_ * height_ * 3); // RGB

  guint64 pts = 0;
  const GstClockTime duration =
      gst_util_uint64_scale_int(GST_SECOND, 1, static_cast<int>(framerate_));

  cv::Mat frame, rgb;

  while (file_running_) {
    auto t0 = std::chrono::steady_clock::now();

    if (!cap.read(frame) || frame.empty()) {
      RCLCPP_DEBUG(get_logger(), "File loop: rewinding '%s'",
                   source_.value.c_str());
      cap.set(cv::CAP_PROP_POS_FRAMES, 0);
      pts = 0;
      continue;
    }

    // Convert to RGB
    if (frame.channels() == 1)
      cv::cvtColor(frame, rgb, cv::COLOR_GRAY2RGB);
    else if (frame.channels() == 4)
      cv::cvtColor(frame, rgb, cv::COLOR_BGRA2RGB);
    else
      cv::cvtColor(frame, rgb, cv::COLOR_BGR2RGB);

    // Resize if needed
    if (rgb.cols != static_cast<int>(width_) ||
        rgb.rows != static_cast<int>(height_)) {
      cv::resize(rgb, rgb, cv::Size(static_cast<int>(width_),
                                    static_cast<int>(height_)));
    }

    // Wrap in GstBuffer and push
    GstBuffer *buf = gst_buffer_new_allocate(nullptr, frame_bytes, nullptr);
    GstMapInfo map;
    gst_buffer_map(buf, &map, GST_MAP_WRITE);
    std::memcpy(map.data, rgb.data, frame_bytes);
    gst_buffer_unmap(buf, &map);

    GST_BUFFER_PTS(buf)      = pts;
    GST_BUFFER_DURATION(buf) = duration;
    pts += duration;

    GstFlowReturn ret = gst_app_src_push_buffer(file_appsrc_, buf);
    // buf is consumed by push_buffer — do not unref

    if (ret == GST_FLOW_FLUSHING) {
      // Pipeline is being torn down (client disconnected).
      // Sleep briefly and keep looping — on_media_configure() will
      // set a new appsrc and file_running_ stays true for the next client.
      RCLCPP_DEBUG(get_logger(), "File loop: pipeline flushing, waiting for new client");
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }

    if (ret != GST_FLOW_OK && file_running_) {
      RCLCPP_WARN(get_logger(), "File loop: appsrc push returned %d — exiting", ret);
      break;
    }

    // Sleep to honour frame rate
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now() - t0)
                       .count();
    long sleep_us = frame_period_us - elapsed;
    if (sleep_us > 0)
      std::this_thread::sleep_for(std::chrono::microseconds(sleep_us));
  }

  cap.release();
  RCLCPP_INFO(get_logger(), "File loop thread exiting");
}

void stop_file_thread() {
  file_running_ = false;
  if (file_thread_.joinable()) file_thread_.join();
  if (file_appsrc_) {
    gst_object_unref(file_appsrc_);
    file_appsrc_ = nullptr;
  }
}

  // ── appsink callback ──────────────────────────────────────────────────────
  static GstFlowReturn on_new_sample_static(GstAppSink *sink, gpointer ud) {
    return static_cast<RTSPCameraStreamer *>(ud)->on_new_sample(sink);
  }

  GstFlowReturn on_new_sample(GstAppSink *sink) {
    GstSample *sample = gst_app_sink_pull_sample(sink);
    if (!sample) return GST_FLOW_ERROR;

    GstBuffer    *buf = gst_sample_get_buffer(sample);
    GstStructure *st  = gst_caps_get_structure(gst_sample_get_caps(sample), 0);

    int w = 0, h = 0;
    gst_structure_get_int(st, "width",  &w);
    gst_structure_get_int(st, "height", &h);

    GstMapInfo map;
    if (gst_buffer_map(buf, &map, GST_MAP_READ)) {
      auto stamp = now();

      auto img        = std::make_shared<sensor_msgs::msg::Image>();
      img->header.stamp    = stamp;
      img->header.frame_id = frame_id_;
      img->width       = static_cast<uint32_t>(w);
      img->height      = static_cast<uint32_t>(h);
      img->encoding    = "rgb8";
      img->is_bigendian = 0;
      img->step        = static_cast<uint32_t>(w * 3);
      img->data.assign(map.data, map.data + map.size);
      image_pub_.publish(img);

      auto ci = std::make_shared<sensor_msgs::msg::CameraInfo>(
          camera_info_manager_->getCameraInfo());
      ci->header.stamp    = stamp;
      ci->header.frame_id = frame_id_;
      ci->width  = static_cast<uint32_t>(w);
      ci->height = static_cast<uint32_t>(h);
      camera_info_pub_->publish(*ci);

      gst_buffer_unmap(buf, &map);
    }

    gst_sample_unref(sample);
    return GST_FLOW_OK;
  }

  // ── Device discovery ──────────────────────────────────────────────────────
  static bool usb_matches(const gchar *val, const std::string &port) {
    return val && std::string(val).find(port) != std::string::npos;
  }

  GstDevice *find_camera_by_usb_port(const std::string &usb_port,
                                     std::string &device_path) {
    GstDeviceMonitor *monitor = gst_device_monitor_new();
    gst_device_monitor_add_filter(monitor, "Video/Source", nullptr);
    GList *devices = gst_device_monitor_get_devices(monitor);
    GstDevice *result = nullptr;

    for (GList *l = devices; l && !result; l = l->next) {
      GstDevice    *dev   = GST_DEVICE(l->data);
      GstStructure *props = gst_device_get_properties(dev);
      if (!props) continue;

      const gchar *bus  = gst_structure_get_string(props, "device.bus_path");
      const gchar *sys  = gst_structure_get_string(props, "sysfs.path");
      const gchar *info = gst_structure_get_string(props, "v4l2.device.bus_info");

      if (usb_matches(bus, usb_port) || usb_matches(sys, usb_port) ||
          usb_matches(info, usb_port)) {
        const gchar *p = gst_structure_get_string(props, "api.v4l2.path");
        if (!p) p      = gst_structure_get_string(props, "device.path");
        if (p && device_path.empty()) device_path = p;
        result = GST_DEVICE(gst_object_ref(dev));
        RCLCPP_INFO(get_logger(), "USB match: %s → %s",
                    usb_port.c_str(), device_path.c_str());
      }
      gst_structure_free(props);
    }

    if (!result)
      RCLCPP_WARN(get_logger(), "No device at USB port '%s'", usb_port.c_str());

    g_list_free_full(devices, gst_object_unref);
    gst_device_monitor_stop(monitor);
    gst_object_unref(monitor);
    return result;
  }

  GstDevice *find_camera_by_id(long vendor, long product,
                                const std::string &serial) {
    if (vendor == -1 || product == -1 || serial.empty()) {
      RCLCPP_WARN(get_logger(), "Invalid vendor/product/serial — skipping");
      return nullptr;
    }
    GstDeviceMonitor *monitor = gst_device_monitor_new();
    gst_device_monitor_add_filter(monitor, "Video/Source", nullptr);
    if (!gst_device_monitor_start(monitor)) {
      gst_object_unref(monitor); return nullptr;
    }
    GList *devices = gst_device_monitor_get_devices(monitor);
    GstDevice *result = nullptr;

    for (GList *l = devices; l && !result; l = l->next) {
      GstDevice    *dev   = GST_DEVICE(l->data);
      GstStructure *props = gst_device_get_properties(dev);
      if (!props) continue;

      const gchar *vs = gst_structure_get_string(props, "device.vendor.id");
      const gchar *ps = gst_structure_get_string(props, "device.product.id");
      const gchar *sn = gst_structure_get_string(props, "device.serial");

      if (vs && ps && sn) {
        bool match = (vendor  == 0 || strtol(vs, nullptr, 16) == vendor) &&
                     (product == 0 || strtol(ps, nullptr, 16) == product) &&
                     (serial.empty() || std::string(sn).find(serial) != std::string::npos);
        if (match) result = GST_DEVICE(gst_object_ref(dev));
      }
      gst_structure_free(props);
    }

    g_list_free_full(devices, gst_object_unref);
    gst_device_monitor_stop(monitor);
    gst_object_unref(monitor);
    return result;
  }

  GstDevice *find_camera_by_path(const std::string &device_path) {
    if (device_path.empty()) return nullptr;
    GstDeviceMonitor *monitor = gst_device_monitor_new();
    gst_device_monitor_add_filter(monitor, "Video/Source", nullptr);
    if (!gst_device_monitor_start(monitor)) {
      gst_object_unref(monitor); return nullptr;
    }
    GList *devices = gst_device_monitor_get_devices(monitor);
    GstDevice *result = nullptr;

    for (GList *l = devices; l && !result; l = l->next) {
      GstDevice    *dev   = GST_DEVICE(l->data);
      GstStructure *props = gst_device_get_properties(dev);
      if (!props) continue;
      const gchar *p = gst_structure_get_string(props, "api.v4l2.path");
      if (!p) p      = gst_structure_get_string(props, "device.path");
      if (p && device_path == p) result = GST_DEVICE(gst_object_ref(dev));
      gst_structure_free(props);
    }

    g_list_free_full(devices, gst_object_unref);
    gst_device_monitor_stop(monitor);
    gst_object_unref(monitor);
    return result;
  }

  GstDevice *find_camera_any(std::string &device_path) {
    GstDeviceMonitor *monitor = gst_device_monitor_new();
    gst_device_monitor_add_filter(monitor, "Video/Source", nullptr);
    if (!gst_device_monitor_start(monitor)) {
      gst_object_unref(monitor); return nullptr;
    }
    GList *devices = gst_device_monitor_get_devices(monitor);
    if (!devices) {
      gst_device_monitor_stop(monitor);
      gst_object_unref(monitor);
      return nullptr;
    }
    GstDevice *result = GST_DEVICE(gst_object_ref(g_list_first(devices)->data));
    GstStructure *props = gst_device_get_properties(result);
    if (props) {
      const gchar *p = gst_structure_get_string(props, "api.v4l2.path");
      if (!p) p      = gst_structure_get_string(props, "device.path");
      if (p) device_path = p;
      gst_structure_free(props);
    }
    g_list_free_full(devices, gst_object_unref);
    gst_device_monitor_stop(monitor);
    gst_object_unref(monitor);
    return result;
  }

  // ── Capability query ──────────────────────────────────────────────────────
  void query_camera_capabilities(GstDevice *dev,
                                 std::set<std::string> &formats,
                                 std::set<std::pair<int,int>> &resolutions) {
    GstCaps *caps = gst_device_get_caps(dev);
    if (!caps) return;

    for (guint i = 0; i < gst_caps_get_size(caps); i++) {
      GstStructure *s     = gst_caps_get_structure(caps, i);
      const gchar  *mtype = gst_structure_get_name(s);

      if      (g_strcmp0(mtype, "image/jpeg")   == 0) formats.insert("MJPEG");
      else if (g_strcmp0(mtype, "video/x-h264") == 0) formats.insert("H264");
      else if (g_strcmp0(mtype, "video/x-raw")  == 0) {
        const gchar *f = gst_structure_get_string(s, "format");
        if      (g_strcmp0(f, "YUY2") == 0) formats.insert("YUYV");
        else if (g_strcmp0(f, "NV12") == 0) formats.insert("NV12");
        else if (g_strcmp0(f, "RGB")  == 0) formats.insert("RGB");
        else if (g_strcmp0(f, "BGR")  == 0) formats.insert("BGR");
      }

      int w = 0, h = 0;
      if (gst_structure_get_int(s, "width", &w) &&
          gst_structure_get_int(s, "height", &h))
        resolutions.insert({w, h});
    }

    if (!formats.empty()) {
      std::ostringstream os;
      for (auto &f : formats) os << f << " ";
      RCLCPP_INFO(get_logger(), "Supported formats: %s", os.str().c_str());
    }
    if (!resolutions.empty()) {
      std::ostringstream os;
      for (auto &r : resolutions) os << r.first << "x" << r.second << " ";
      RCLCPP_INFO(get_logger(), "Supported resolutions: %s", os.str().c_str());
    }
    gst_caps_unref(caps);
  }

  // ── Static helpers ────────────────────────────────────────────────────────
  static std::string gst_caps_for_format(const std::string &fmt) {
    if (strcasecmp(fmt.c_str(), "MJPEG") == 0) return "image/jpeg";
    if (strcasecmp(fmt.c_str(), "YUYV") == 0)  return "video/x-raw,format=YUY2";
    if (strcasecmp(fmt.c_str(), "H264") == 0)  return "video/x-h264";
    if (strcasecmp(fmt.c_str(), "RGB")  == 0)  return "video/x-raw,format=RGB";
    if (strcasecmp(fmt.c_str(), "BGR")  == 0)  return "video/x-raw,format=BGR";
    if (strcasecmp(fmt.c_str(), "NV12") == 0)  return "video/x-raw,format=NV12";
    return "image/jpeg";
  }

  void log_configuration(const std::string &pipeline_str) const {
    const char *ip = get_machine_ip();
    RCLCPP_INFO(get_logger(), "============================================================");
    RCLCPP_INFO(get_logger(), "RTSP Stream Configuration");
    RCLCPP_INFO(get_logger(), "============================================================");
    RCLCPP_INFO(get_logger(), "Source type:        %s",
                source_.type == SourceType::File   ? ("file: " + source_.value).c_str() :
                source_.type == SourceType::RTSP   ? ("rtsp: " + source_.value).c_str() :
                source_.type == SourceType::USB    ? ("usb:  " + source_.value).c_str() :
                source_.type == SourceType::Device ? ("dev:  " + source_.value).c_str() :
                                                      "legacy (v4l2)");
    RCLCPP_INFO(get_logger(), "Resolution:         %ldx%ld", width_, height_);
    RCLCPP_INFO(get_logger(), "Framerate:          %ld fps", framerate_);
    RCLCPP_INFO(get_logger(), "Bitrate:            %ld kbps", bitrate_);
    RCLCPP_INFO(get_logger(), "RTSP URL:           rtsp://%s:%ld%s", ip, port_, mount_point_);
    RCLCPP_INFO(get_logger(), "ROS2 topic:         %s/image", ros_topic_basename_.c_str());
    RCLCPP_INFO(get_logger(), "Play with:          ffplay -fflags nobuffer -flags low_delay"
                              " -framedrop rtsp://%s:%ld%s", ip, port_, mount_point_);
    RCLCPP_INFO(get_logger(), "============================================================");
    (void)pipeline_str; // already logged by caller
  }

  // ── Cleanup ───────────────────────────────────────────────────────────────
  void cleanup() {
    stop_file_thread();

    if (server_) { g_object_unref(server_); server_ = nullptr; }
    if (loop_)     g_main_loop_quit(loop_);
    if (glib_thread_.joinable()) glib_thread_.join();
    if (loop_)   { g_main_loop_unref(loop_);     loop_         = nullptr; }
    if (glib_context_) { g_main_context_unref(glib_context_); glib_context_ = nullptr; }
  }

  // ── Members ───────────────────────────────────────────────────────────────
  // GStreamer / RTSP
  GstRTSPServer *server_       = nullptr;
  GMainContext  *glib_context_ = nullptr;
  GMainLoop     *loop_         = nullptr;
  std::thread    glib_thread_;

  // File-source loop
  GstAppSrc        *file_appsrc_  = nullptr;
  std::thread       file_thread_;
  std::atomic<bool> file_running_{false};

  // ROS 2
  image_transport::Publisher                               image_pub_;
  rclcpp::Publisher<sensor_msgs::msg::CameraInfo>::SharedPtr camera_info_pub_;
  std::shared_ptr<camera_info_manager::CameraInfoManager>   camera_info_manager_;

  // Config
  ParsedSource source_;
  long         width_     = 640;
  long         height_    = 480;
  long         framerate_ = 30;
  long         bitrate_   = 2000;
  long         port_      = 8554;
  std::string  fmt_       = "MJPEG";
  std::string  frame_id_;
  std::string  ros_topic_basename_;
  std::string  crop_param_;

  static constexpr const char *mount_point_ = "/image_rtsp";
};

// ── main ──────────────────────────────────────────────────────────────────

int main(int argc, char **argv) {
  rclcpp::init(argc, argv);
  try {
    auto node = std::make_shared<RTSPCameraStreamer>();
    node->initialize();
    rclcpp::spin(node);
  } catch (const std::exception &e) {
    RCLCPP_ERROR(rclcpp::get_logger("rtsp_camera_streamer"), "Fatal: %s", e.what());
    rclcpp::shutdown();
    return 1;
  }
  rclcpp::shutdown();
  return 0;
}