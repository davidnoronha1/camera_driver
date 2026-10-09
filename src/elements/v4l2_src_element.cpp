#include "camera_driver/elements/v4l2_src_element.hpp"
#include "camera_driver/element_registry.hpp"
#include "camera_driver/lflogger.hpp"
#include <atomic>
#include <fcntl.h>
#include <fmt/format.h>
#include <stdexcept>
#include <unistd.h>

namespace camera_driver {

namespace {

static std::atomic<int> g_v4l2_id{0};

// Try to find a device matching a criterion from the scanned device list.
// Returns the device path, or empty string on failure.
std::string tryMatchCriterion(const SelectionCriterion& criterion,
                               const std::vector<v4l2probe::DeviceProbeInfo>& devices)
{
    return std::visit([&](auto&& c) -> std::string {
        using T = std::decay_t<decltype(c)>;

        if constexpr (std::is_same_v<T, BySerial>) {
            for (const auto& d : devices)
                if (d.serial == c.serial) return d.path;
            LockFreeLogger::getInstance().warn("v4l2_src",
                fmt::format("BySerial: no device with serial '{}'", c.serial));
            return "";
        }

        if constexpr (std::is_same_v<T, ByPort>) {
            for (const auto& d : devices)
                if (d.bus_info == c.bus_id) return d.path;
            LockFreeLogger::getInstance().warn("v4l2_src",
                fmt::format("ByPort: no device with bus_id '{}'", c.bus_id));
            return "";
        }

        if constexpr (std::is_same_v<T, ByFormat>) {
            for (const auto& d : devices) {
                int fd = open(d.path.c_str(), O_RDWR | O_NONBLOCK);
                if (fd < 0) continue;
                auto formats = v4l2probe::enumerateFormats(fd);
                close(fd);
                for (const auto& fi : formats) {
                    if (fi.fourcc != c.fourcc) continue;
                    for (const auto& sz : fi.sizes) {
                        if ((c.width == 0 || sz.width == static_cast<uint32_t>(c.width)) &&
                            (c.height == 0 || sz.height == static_cast<uint32_t>(c.height))) {
                            for (uint32_t fps : sz.fps_list)
                                if (c.fps == 0 || fps == static_cast<uint32_t>(c.fps))
                                    return d.path;
                        }
                    }
                }
            }
            LockFreeLogger::getInstance().warn("v4l2_src",
                fmt::format("ByFormat: no device supports {}x{}@{} {}",
                    c.width, c.height, c.fps, v4l2probe::fourccToString(c.fourcc)));
            return "";
        }

        if constexpr (std::is_same_v<T, Interactive>) {
            // Always succeeds (user picks)
            return "interactive";
        }

        return "";
    }, criterion);
}

std::string buildGstSrcString(const std::string& device_path,
                               PixelFormat chosen_fmt,
                               int width, int height, int fps)
{
    std::string caps;
    std::string post;

    switch (chosen_fmt) {
        case PixelFormat::MJPEG:
            caps = fmt::format("image/jpeg,width={},height={},framerate={}/1", width, height, fps);
            post = " ! jpegparse";
            break;
        case PixelFormat::H264:
            caps = fmt::format("video/x-h264,width={},height={},framerate={}/1", width, height, fps);
            post = " ! h264parse";
            break;
        case PixelFormat::NV12:
            caps = fmt::format("video/x-raw,format=NV12,width={},height={},framerate={}/1", width, height, fps);
            break;
        case PixelFormat::YUYV:
        default:
            caps = fmt::format("video/x-raw,format=YUY2,width={},height={},framerate={}/1", width, height, fps);
            break;
    }

    return fmt::format("v4l2src device={} do-timestamp=true ! {}{}", device_path, caps, post);
}

// Pick the best output format for the camera given downstream preferences and available formats.
PixelFormat pickOutputFormat(const std::vector<PixelFormat>& downstream_prefs,
                              const std::vector<v4l2probe::FormatInfo>& formats,
                              int width, int height, int fps,
                              bool /*nvidia*/)
{
    auto supportsFormat = [&](uint32_t fourcc) {
        for (const auto& fi : formats) {
            if (fi.fourcc != fourcc) continue;
            for (const auto& sz : fi.sizes) {
                if ((width == 0 || sz.width == static_cast<uint32_t>(width)) &&
                    (height == 0 || sz.height == static_cast<uint32_t>(height))) {
                    if (sz.fps_list.empty()) return true;
                    for (uint32_t f : sz.fps_list)
                        if (fps == 0 || f == static_cast<uint32_t>(fps)) return true;
                }
            }
        }
        return false;
    };

    for (PixelFormat pref : downstream_prefs) {
        switch (pref) {
            case PixelFormat::MJPEG:
                if (supportsFormat(V4L2_PIX_FMT_MJPEG)) return PixelFormat::MJPEG;
                break;
            case PixelFormat::H264: case PixelFormat::H264_NVMM:
                if (supportsFormat(V4L2_PIX_FMT_H264)) return PixelFormat::H264;
                break;
            case PixelFormat::NV12: case PixelFormat::NV12_NVMM:
                if (supportsFormat(V4L2_PIX_FMT_NV12)) return PixelFormat::NV12;
                break;
            default:
                break;
        }
    }

    // Fallback: YUYV is most universally supported
    if (supportsFormat(V4L2_PIX_FMT_YUYV)) return PixelFormat::YUYV;
    if (supportsFormat(V4L2_PIX_FMT_MJPEG)) return PixelFormat::MJPEG;

    // Last resort: pick first available
    if (!formats.empty()) {
        uint32_t fc = formats[0].fourcc;
        if (fc == V4L2_PIX_FMT_MJPEG) return PixelFormat::MJPEG;
        if (fc == V4L2_PIX_FMT_NV12)  return PixelFormat::NV12;
        if (fc == V4L2_PIX_FMT_H264)  return PixelFormat::H264;
    }
    return PixelFormat::YUYV;
}

// Get actual resolution from camera formats (fills in width/height if they are 0)
std::pair<int,int> resolveResolution(const std::vector<v4l2probe::FormatInfo>& formats,
                                      uint32_t fourcc, int want_w, int want_h)
{
    for (const auto& fi : formats) {
        if (fi.fourcc != fourcc) continue;
        for (const auto& sz : fi.sizes) {
            if ((want_w == 0 || sz.width  == static_cast<uint32_t>(want_w)) &&
                (want_h == 0 || sz.height == static_cast<uint32_t>(want_h))) {
                return {static_cast<int>(sz.width), static_cast<int>(sz.height)};
            }
        }
        // No exact match — return first available
        if (!fi.sizes.empty())
            return {static_cast<int>(fi.sizes[0].width),
                    static_cast<int>(fi.sizes[0].height)};
    }
    return {want_w ? want_w : 640, want_h ? want_h : 480};
}

} // anonymous namespace

V4L2SrcElement::V4L2SrcElement(std::vector<SelectionCriterion> criteria,
                                int width, int height, int fps)
    : UnresolvedSegment("v4l2_src_" + std::to_string(g_v4l2_id++), {})
    , criteria_(std::move(criteria))
    , width_(width), height_(height), fps_(fps)
{
    // Install resolver
    auto* self = this;
    auto resolver = [self](const PipelineContext& ctx) -> ResolvedSegment {
        auto& log = LockFreeLogger::getInstance();

        // Scan all V4L2 devices once
        auto devices = v4l2probe::scanDevices();

        std::string device_path;
        for (const auto& criterion : self->criteria_) {
            if (std::holds_alternative<Interactive>(criterion)) {
                // Interactive: show NVIDIA hints, then call wizard
                bool nvidia = ctx.hw.has_nvjpegenc || ctx.hw.has_nvh264enc || ctx.hw.has_nvvidconv;
                log.info("v4l2_src",
                    fmt::format("Interactive selection{}", nvidia ?
                        " [NVIDIA detected: MJPEG or NV12 preferred to avoid GPU copies]" : ""));
                auto sel = v4l2probe::interactiveSelect();
                if (!sel.valid) throw std::runtime_error("V4L2SrcElement: interactive selection cancelled");
                device_path = sel.device.path;
                break;
            }
            device_path = tryMatchCriterion(criterion, devices);
            if (!device_path.empty()) {
                std::visit([&](auto&& c) {
                    using T = std::decay_t<decltype(c)>;
                    if constexpr (std::is_same_v<T, BySerial>)
                        log.info("v4l2_src", fmt::format("Found device {} via serial '{}'",
                            device_path, c.serial));
                    else if constexpr (std::is_same_v<T, ByPort>)
                        log.info("v4l2_src", fmt::format("Found device {} via port '{}'",
                            device_path, c.bus_id));
                    else if constexpr (std::is_same_v<T, ByFormat>)
                        log.info("v4l2_src", fmt::format("Found device {} via format match",
                            device_path));
                }, criterion);
                break;
            }
        }

        if (device_path.empty())
            throw std::runtime_error("V4L2SrcElement: no device matched any criterion");

        // Enumerate formats on the matched device
        int fd = open(device_path.c_str(), O_RDWR | O_NONBLOCK);
        if (fd < 0) throw std::runtime_error("V4L2SrcElement: cannot open " + device_path);
        auto formats = v4l2probe::enumerateFormats(fd);
        close(fd);

        // Print what's available
        log.info("v4l2_src", fmt::format("Device {} supports {} format(s):",
            device_path, formats.size()));
        for (const auto& fi : formats) {
            std::string sizes_str;
            for (const auto& sz : fi.sizes)
                sizes_str += fmt::format(" {}x{}", sz.width, sz.height);
            log.info("v4l2_src", fmt::format("  {} ({}){}", fi.description,
                v4l2probe::fourccToString(fi.fourcc), sizes_str));
        }

        // Pick output format based on downstream preferences
        bool nvidia = ctx.hw.has_nvjpegenc || ctx.hw.has_nvh264enc || ctx.hw.has_nvvidconv;
        PixelFormat chosen = pickOutputFormat(
            ctx.downstream_prefs, formats, self->width_, self->height_, self->fps_, nvidia);

        // Resolve actual resolution
        uint32_t fourcc = V4L2_PIX_FMT_YUYV;
        switch (chosen) {
            case PixelFormat::MJPEG: fourcc = V4L2_PIX_FMT_MJPEG; break;
            case PixelFormat::H264:  fourcc = V4L2_PIX_FMT_H264;  break;
            case PixelFormat::NV12:  fourcc = V4L2_PIX_FMT_NV12;  break;
            default: break;
        }
        auto [actual_w, actual_h] = resolveResolution(formats, fourcc, self->width_, self->height_);
        int actual_fps = self->fps_ > 0 ? self->fps_ : 30;

        log.info("v4l2_src", fmt::format("Selected: {} {}x{}@{} from {}",
            pixelFormatName(chosen), actual_w, actual_h, actual_fps, device_path));

        ResolvedSegment seg;
        seg.name = self->name_;
        seg.gst_string = buildGstSrcString(device_path, chosen, actual_w, actual_h, actual_fps);
        seg.input_caps.is_any = true;
        seg.output_caps.format = chosen;
        seg.output_caps.width  = actual_w;
        seg.output_caps.height = actual_h;
        seg.output_caps.fps_num = actual_fps;
        seg.output_caps.fps_den = 1;

        // Store available formats in ctx so OptimizedConverter can see them
        // (we return them as part of the resolved segment metadata)
        const_cast<PipelineContext&>(ctx).v4l2_formats = formats;

        return seg;
    };

    // Replace the resolver in our base UnresolvedSegment
    *static_cast<UnresolvedSegment*>(this) = UnresolvedSegment(name_, std::move(resolver), {});
}

void V4L2SrcElement::addCriterion(SelectionCriterion c) {
    criteria_.push_back(std::move(c));
}

} // namespace camera_driver

REGISTER_PIPELINE_ELEMENT(V4L2SrcElement, [](const YAML::Node& cfg) {
    std::vector<camera_driver::SelectionCriterion> criteria;
    if (cfg["selection"] && cfg["selection"].IsSequence()) {
        for (const auto& c : cfg["selection"]) {
            std::string mode = c["mode"].as<std::string>("interactive");
            if      (mode == "serial")
                criteria.push_back(camera_driver::BySerial{c["serial"].as<std::string>()});
            else if (mode == "port")
                criteria.push_back(camera_driver::ByPort{c["bus_id"].as<std::string>()});
            else if (mode == "format")
                criteria.push_back(camera_driver::ByFormat{
                    v4l2probe::fourccFromString(c["fourcc"].as<std::string>("YUYV")),
                    c["width"].as<int>(0), c["height"].as<int>(0), c["fps"].as<int>(0)});
            else
                criteria.push_back(camera_driver::Interactive{});
        }
    } else {
        criteria.push_back(camera_driver::Interactive{});
    }
    return std::make_shared<camera_driver::V4L2SrcElement>(
        criteria,
        cfg["width"].as<int>(0),
        cfg["height"].as<int>(0),
        cfg["fps"].as<int>(30));
});
