#include "camera_driver/elements/optimized_converter.hpp"
#include "camera_driver/element_registry.hpp"
#include "camera_driver/lflogger.hpp"
#include <atomic>
#include <fmt/format.h>
#include <stdexcept>

namespace camera_driver {

namespace {
static std::atomic<int> g_conv_id{0};
} // namespace

std::string OptimizedConverter::resizeStr(const OptimizedVideoResize &r) {
  if (r.width <= 0 || r.height <= 0)
    return "";
  return fmt::format(",width={},height={}", r.width, r.height);
}

OptimizedConverter::OptimizedConverter(
    std::vector<PixelFormat> desired_outputs,
    std::optional<OptimizedVideoResize> resize, int quality, int bitrate_kbps,
    std::vector<std::string> encoder_preferences)
    : UnresolvedSegment("opt_conv_" + std::to_string(g_conv_id++), {}),
      desired_outputs_(std::move(desired_outputs)), resize_(resize),
      quality_(quality), bitrate_kbps_(bitrate_kbps),
      encoder_preferences_(std::move(encoder_preferences)) {
  if (encoder_preferences_.empty()) {
    encoder_preferences_ = {"nvv4l2h264enc", "nvh264enc", "qsvh264enc",
                            "v4l2h264enc", "x264enc"};
  }

  auto *self = this;
  auto resolver = [self](const PipelineContext &ctx) -> ResolvedSegment {
    if (self->desired_outputs_.empty())
      throw std::runtime_error("OptimizedConverter: desired_outputs is empty");

    PixelFormat target = self->desired_outputs_[0]; // highest priority target
    PixelFormat input = ctx.upstream_caps.format;

    auto &log = LockFreeLogger::getInstance();

    // Passthrough check: if input already is the target format
    if (input == target && !self->resize_) {
      ResolvedSegment seg;
      seg.name = self->name_;
      seg.input_caps = ctx.upstream_caps;
      seg.output_caps = ctx.upstream_caps;
      // MJPEG is already parsed by upstream source
      seg.gst_string = "";
      log.info("opt_conv",
               fmt::format("Passthrough {} → {}", pixelFormatName(input),
                           pixelFormatName(target)));
      return seg;
    }

    ResolvedSegment seg;
    seg.name = self->name_;
    seg.input_caps = ctx.upstream_caps;

    switch (target) {
    case PixelFormat::H264:
    case PixelFormat::H264_NVMM:
      seg = buildH264Path(input, ctx, self->bitrate_kbps_, self->resize_,
                          self->encoder_preferences_);
      seg.name = self->name_;
      break;
    case PixelFormat::MJPEG:
      seg = buildMJPEGPath(input, ctx, self->quality_, self->resize_);
      seg.name = self->name_;
      break;
    default:
      throw std::runtime_error(
          fmt::format("OptimizedConverter: unsupported target format {}",
                      pixelFormatName(target)));
    }

    log.info("opt_conv", fmt::format("{} → {} via: {}", pixelFormatName(input),
                                     pixelFormatName(target), seg.gst_string));
    return seg;
  };

  *static_cast<UnresolvedSegment *>(this) =
      UnresolvedSegment(name_, std::move(resolver), preferredInputFormats());
}

ResolvedSegment OptimizedConverter::buildH264Path(
    PixelFormat input, const PipelineContext &ctx, int bitrate_kbps,
    const std::optional<OptimizedVideoResize> &resize,
    const std::vector<std::string> &encoder_prefs) {
  ResolvedSegment seg;
  seg.output_caps.format = PixelFormat::H264;

  auto bitrateStr = [&](const std::string &enc_flag) -> std::string {
    if (bitrate_kbps > 0)
      return fmt::format(" {}={}", enc_flag, bitrate_kbps);
    return "";
  };

  auto resize_s = resize ? resizeStr(*resize) : std::string{};

  std::string chosen_enc = "";
  for (const auto &enc : encoder_prefs) {
    if (enc == "nvv4l2h264enc" && ctx.hw.has_nvvidconv &&
        ctx.hw.has_nvv4l2h264enc) {
      chosen_enc = enc;
      break;
    }
    if (enc == "nvh264enc" && ctx.hw.has_nvh264enc) {
      chosen_enc = enc;
      break;
    }
    if (enc == "qsvh264enc" && ctx.hw.has_qsvh264enc) {
      chosen_enc = enc;
      break;
    }
    if (enc == "v4l2h264enc" && ctx.hw.has_v4l2h264enc) {
      chosen_enc = enc;
      break;
    }
    if (enc == "x264enc" && ctx.hw.has_x264enc) {
      chosen_enc = enc;
      break;
    }
  }

  if (chosen_enc.empty()) {
    throw std::runtime_error("OptimizedConverter: no H264 encoder available "
                             "(install gstreamer1.0-plugins-ugly for x264enc)");
  }

  if (chosen_enc == "nvv4l2h264enc") {
    // NVMM path: colorspace converter → nvv4l2h264enc
    const std::string &nvc = ctx.hw.nvvidconv_name;
    std::string conv_step;
    switch (input) {
    case PixelFormat::NV12_NVMM:
      conv_step = resize_s.empty()
                      ? ""
                      : nvc + " ! video/x-raw(memory:NVMM),format=NV12" +
                            resize_s + " ! ";
      break;
    case PixelFormat::NV12:
      conv_step =
          nvc + " ! video/x-raw(memory:NVMM),format=NV12" + resize_s + " ! ";
      break;
    case PixelFormat::MJPEG:
      conv_step = "jpegdec ! " + nvc +
                  " ! video/x-raw(memory:NVMM),format=NV12" + resize_s + " ! ";
      break;
    default:
      conv_step = "videoconvert ! video/x-raw,format=NV12 ! " + nvc +
                  " ! video/x-raw(memory:NVMM),format=NV12" + resize_s + " ! ";
      break;
    }
    seg.gst_string = conv_step + "nvv4l2h264enc" + bitrateStr("bitrate");

  } else if (chosen_enc == "nvh264enc") {
    // Desktop NVIDIA: videoconvert → NV12 → nvh264enc
    std::string conv_step;
    switch (input) {
    case PixelFormat::NV12:
      conv_step = !resize_s.empty() ? "videoscale ! video/x-raw,format=NV12" +
                                          resize_s + " ! "
                                    : "";
      break;
    case PixelFormat::MJPEG:
      conv_step =
          "jpegdec ! videoconvert ! video/x-raw,format=NV12" + resize_s + " ! ";
      break;
    default:
      conv_step = "videoconvert ! video/x-raw,format=NV12";
      if (!resize_s.empty())
        conv_step += ",width=" + std::to_string(resize->width) +
                     ",height=" + std::to_string(resize->height);
      conv_step += " ! ";
      break;
    }
    seg.gst_string = conv_step + "nvh264enc" + bitrateStr("bitrate");

  } else if (chosen_enc == "qsvh264enc") {
    std::string conv_step = (input == PixelFormat::MJPEG) ? "jpegdec ! " : "";
    conv_step += "videoconvert ! video/x-raw,format=I420";
    if (!resize_s.empty())
      conv_step += ",width=" + std::to_string(resize->width) +
                   ",height=" + std::to_string(resize->height);
    conv_step += " ! ";
    seg.gst_string = conv_step + "qsvh264enc" + bitrateStr("bitrate");

  } else if (chosen_enc == "v4l2h264enc") {
    std::string conv_step = (input == PixelFormat::MJPEG) ? "jpegdec ! " : "";
    conv_step += "videoconvert ! video/x-raw,format=I420";
    if (!resize_s.empty())
      conv_step += ",width=" + std::to_string(resize->width) +
                   ",height=" + std::to_string(resize->height);
    conv_step += " ! ";
    seg.gst_string = conv_step + "v4l2h264enc ! video/x-h264,level=(string)4";

  } else if (chosen_enc == "x264enc") {
    std::string conv_step = (input == PixelFormat::MJPEG) ? "jpegdec ! " : "";
    conv_step += "videoconvert ! video/x-raw,format=I420";
    if (!resize_s.empty())
      conv_step += ",width=" + std::to_string(resize->width) +
                   ",height=" + std::to_string(resize->height);
    conv_step += " ! ";
    std::string enc = "x264enc tune=zerolatency speed-preset=ultrafast";
    if (bitrate_kbps > 0)
      enc += fmt::format(" bitrate={}", bitrate_kbps);
    seg.gst_string = conv_step + enc;
  }

  return seg;
}

ResolvedSegment OptimizedConverter::buildMJPEGPath(
    PixelFormat input, const PipelineContext &ctx, int quality,
    const std::optional<OptimizedVideoResize> &resize) {
  ResolvedSegment seg;
  seg.output_caps.format = PixelFormat::MJPEG;

  auto resize_s = resize ? resizeStr(*resize) : std::string{};

  if (input == PixelFormat::MJPEG) {
    // Passthrough — already has framing/parser upstream
    seg.gst_string = "";
    return seg;
  }

  std::string pre;
  if (input == PixelFormat::NV12_NVMM && ctx.hw.has_nvvidconv) {
    const std::string &nvc = ctx.hw.nvvidconv_name;
    if (ctx.hw.has_nvjpegenc)
      pre = !resize_s.empty()
                ? nvc + " ! video/x-raw(memory:NVMM),format=I420" + resize_s +
                      " ! "
                : "";
    else
      pre = nvc + " ! video/x-raw,format=I420";
  } else {
    pre = "videoconvert ! video/x-raw,format=I420";
    if (!resize_s.empty())
      pre += ",width=" + std::to_string(resize->width) +
             ",height=" + std::to_string(resize->height);
    pre += " ! ";
  }

  if (ctx.hw.has_nvjpegenc) {
    seg.gst_string =
        pre + fmt::format("nvjpegenc quality={} ! jpegparse", quality);
  } else if (ctx.hw.has_jpegenc) {
    seg.gst_string =
        pre + fmt::format("jpegenc quality={} ! jpegparse", quality);
  } else {
    throw std::runtime_error("OptimizedConverter: no JPEG encoder available");
  }

  return seg;
}

std::vector<PixelFormat> OptimizedConverter::preferredInputFormats() const {
  if (desired_outputs_.empty())
    return {};

  PixelFormat target = desired_outputs_[0];
  const hw::HWCaps &hw = hw::probe();

  if (target == PixelFormat::H264 || target == PixelFormat::H264_NVMM) {
    std::string chosen_enc = "";
    for (const auto &enc : encoder_preferences_) {
      if (enc == "nvv4l2h264enc" && hw.has_nvvidconv && hw.has_nvv4l2h264enc) {
        chosen_enc = enc;
        break;
      }
      if (enc == "nvh264enc" && hw.has_nvh264enc) {
        chosen_enc = enc;
        break;
      }
      if (enc == "qsvh264enc" && hw.has_qsvh264enc) {
        chosen_enc = enc;
        break;
      }
      if (enc == "v4l2h264enc" && hw.has_v4l2h264enc) {
        chosen_enc = enc;
        break;
      }
      if (enc == "x264enc" && hw.has_x264enc) {
        chosen_enc = enc;
        break;
      }
    }

    if (chosen_enc == "nvv4l2h264enc")
      return {PixelFormat::NV12_NVMM, PixelFormat::NV12, PixelFormat::YUYV,
              PixelFormat::MJPEG};
    if (chosen_enc == "nvh264enc")
      return {PixelFormat::NV12, PixelFormat::YUYV, PixelFormat::MJPEG};
    return {PixelFormat::YUYV, PixelFormat::I420, PixelFormat::MJPEG};
  }

  if (target == PixelFormat::MJPEG) {
    if (hw.has_nvjpegenc)
      return {PixelFormat::MJPEG, PixelFormat::NV12_NVMM, PixelFormat::NV12,
              PixelFormat::YUYV};
    return {PixelFormat::MJPEG, PixelFormat::YUYV, PixelFormat::NV12};
  }

  return {PixelFormat::YUYV, PixelFormat::NV12, PixelFormat::MJPEG};
}

} // namespace camera_driver

REGISTER_PIPELINE_ELEMENT(OptimizedConverter, [](const YAML::Node &cfg) {
  std::vector<camera_driver::PixelFormat> outputs;
  if (cfg["outputs"] && cfg["outputs"].IsSequence())
    for (auto &f : cfg["outputs"])
      outputs.push_back(
          camera_driver::pixelFormatFromString(f.as<std::string>()));

  std::optional<camera_driver::OptimizedVideoResize> resize;
  if (cfg["resize"])
    resize = camera_driver::OptimizedVideoResize{
        cfg["resize"]["width"].as<int>(0), cfg["resize"]["height"].as<int>(0)};

  std::vector<std::string> encoders;
  if (cfg["encoders"] && cfg["encoders"].IsSequence()) {
    for (auto &e : cfg["encoders"]) {
      encoders.push_back(e.as<std::string>());
    }
  }

  return std::make_shared<camera_driver::OptimizedConverter>(
      outputs, resize, cfg["quality"].as<int>(85),
      cfg["bitrate_kbps"].as<int>(0), encoders);
});
