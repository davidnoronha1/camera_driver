#pragma once

#include <cstddef>
#include <cstdint>

namespace camera_driver {

// Raw frame layout published into an iceoryx shared-memory loan:
//   [header: kIceOryxFrameHeaderSize bytes][pixel data: data_size bytes]
// Total loan size = kIceOryxFrameHeaderSize + data_size. Plain POD + memcpy,
// no schema/serialization — mirrors the sibling orbbec_iceoryx project's
// orbbec::FrameData.
struct alignas(64) IceOryxFrame {
    uint64_t timestamp_ns;    // [0 ..7 ]  capture time (ns)
    uint64_t sequence_number; // [8 ..15]  monotonic per-publisher counter
    uint32_t width;           // [16..19]  pixels
    uint32_t height;          // [20..23]  pixels
    uint32_t pixel_format;    // [24..27]  camera_driver::PixelFormat value
    uint32_t _pad;            // [28..31]  explicit padding -> data_size at offset 32
    uint64_t data_size;       // [32..39]  bytes of pixel data
    uint8_t  data[1];         // [40]      variable-length payload start
};

inline constexpr std::size_t kIceOryxFrameHeaderSize = 40U;

} // namespace camera_driver
