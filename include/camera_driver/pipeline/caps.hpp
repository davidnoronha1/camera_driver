#pragma once

#include <string>

namespace camera_driver {

enum class PixelFormat {
    Unknown,
    YUYV,
    NV12,
    NV12_NVMM, // NV12 in NVIDIA shared GPU memory
    I420,
    RGB,
    BGR,
    RGBA,
    MJPEG,
    H264,
    H264_NVMM, // H.264 bitstream in NVIDIA memory
    BayerRGGB,
    BayerBGGR,
    BayerGRBG,
    BayerGBRG,
};

struct Caps {
    PixelFormat format  = PixelFormat::Unknown;
    int width           = 0;
    int height          = 0;
    int fps_num         = 30;
    int fps_den         = 1;
    bool is_nvmm        = false; // buffer is in GPU memory
    bool is_any         = false; // wildcard: accepts/produces any format

    bool compatibleWith(const Caps& downstream) const {
        if (is_any || downstream.is_any) return true;
        if (format == PixelFormat::Unknown || downstream.format == PixelFormat::Unknown) return true;
        return format == downstream.format;
    }

    std::string toGstCapsString() const;
    std::string formatName() const;
};

inline std::string pixelFormatName(PixelFormat f) {
    switch (f) {
        case PixelFormat::YUYV:     return "YUYV";
        case PixelFormat::NV12:     return "NV12";
        case PixelFormat::NV12_NVMM: return "NV12(NVMM)";
        case PixelFormat::I420:     return "I420";
        case PixelFormat::RGB:      return "RGB";
        case PixelFormat::BGR:      return "BGR";
        case PixelFormat::RGBA:     return "RGBA";
        case PixelFormat::MJPEG:    return "MJPEG";
        case PixelFormat::H264:     return "H264";
        case PixelFormat::H264_NVMM: return "H264(NVMM)";
        case PixelFormat::BayerRGGB: return "BayerRGGB";
        case PixelFormat::BayerBGGR: return "BayerBGGR";
        case PixelFormat::BayerGRBG: return "BayerGRBG";
        case PixelFormat::BayerGBRG: return "BayerGBRG";
        default:                    return "Unknown";
    }
}

inline PixelFormat pixelFormatFromString(const std::string& s) {
    if (s == "YUYV")     return PixelFormat::YUYV;
    if (s == "NV12")     return PixelFormat::NV12;
    if (s == "I420")     return PixelFormat::I420;
    if (s == "RGB")      return PixelFormat::RGB;
    if (s == "BGR")      return PixelFormat::BGR;
    if (s == "RGBA")     return PixelFormat::RGBA;
    if (s == "MJPEG")    return PixelFormat::MJPEG;
    if (s == "H264")     return PixelFormat::H264;
    if (s == "BayerRGGB") return PixelFormat::BayerRGGB;
    if (s == "BayerBGGR") return PixelFormat::BayerBGGR;
    if (s == "BayerGRBG") return PixelFormat::BayerGRBG;
    if (s == "BayerGBRG") return PixelFormat::BayerGBRG;
    return PixelFormat::Unknown;
}

inline std::string Caps::formatName() const { return pixelFormatName(format); }

inline std::string Caps::toGstCapsString() const {
    if (is_any) return "ANY";
    std::string base;
    switch (format) {
        case PixelFormat::MJPEG:
            base = "image/jpeg";
            break;
        case PixelFormat::H264:
            base = "video/x-h264";
            break;
        case PixelFormat::H264_NVMM:
            base = "video/x-h264(memory:NVMM)";
            break;
        case PixelFormat::NV12_NVMM:
            base = "video/x-raw(memory:NVMM),format=NV12";
            break;
        case PixelFormat::NV12:
            base = "video/x-raw,format=NV12";
            break;
        case PixelFormat::I420:
            base = "video/x-raw,format=I420";
            break;
        case PixelFormat::YUYV:
            base = "video/x-raw,format=YUY2";
            break;
        case PixelFormat::RGB:
            base = "video/x-raw,format=RGB";
            break;
        case PixelFormat::BGR:
            base = "video/x-raw,format=BGR";
            break;
        case PixelFormat::RGBA:
            base = "video/x-raw,format=RGBA";
            break;
        case PixelFormat::BayerRGGB:
            base = "video/x-bayer,format=rggb";
            break;
        case PixelFormat::BayerBGGR:
            base = "video/x-bayer,format=bggr";
            break;
        case PixelFormat::BayerGRBG:
            base = "video/x-bayer,format=grbg";
            break;
        case PixelFormat::BayerGBRG:
            base = "video/x-bayer,format=gbrg";
            break;
        default:
            return "ANY";
    }
    if (width > 0 && height > 0)
        base += ",width=" + std::to_string(width) + ",height=" + std::to_string(height);
    if (fps_num > 0 && fps_den > 0)
        base += ",framerate=" + std::to_string(fps_num) + "/" + std::to_string(fps_den);
    return base;
}

} // namespace camera_driver
