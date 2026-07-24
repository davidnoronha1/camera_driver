#include "red_tint.hpp"

#include <cuda_runtime.h>
#include <cstdint>

namespace camera_driver_demo {

namespace {

// BT.601 (video-range) chroma of pure red (255, 0, 0).
constexpr int kRedCb = 90;
constexpr int kRedCr = 240;

__global__ void redTintKernel(uint8_t* uv, size_t pitch, int row_bytes, int height, float alpha) {
    int pair = blockIdx.x * blockDim.x + threadIdx.x; // one U,V byte pair per thread
    int y    = blockIdx.y * blockDim.y + threadIdx.y;
    if (pair * 2 + 1 >= row_bytes || y >= height) return;

    uint8_t* row = uv + static_cast<size_t>(y) * pitch;
    uint8_t& u = row[pair * 2];
    uint8_t& v = row[pair * 2 + 1];
    u = static_cast<uint8_t>(u + alpha * (kRedCb - u));
    v = static_cast<uint8_t>(v + alpha * (kRedCr - v));
}

} // namespace

void applyRedTint(const camera_driver::CudaFrame& frame, float alpha) {
    using camera_driver::PixelFormat;
    if (frame.format != PixelFormat::NV12 || frame.planes.size() != 2) return;

    const auto& uv = frame.planes[1];
    if (uv.array || uv.ptr == 0) return; // block-linear plane; not handled by this demo

    dim3 block(16, 16);
    dim3 grid((uv.width / 2 + block.x - 1) / block.x, (uv.height + block.y - 1) / block.y);
    redTintKernel<<<grid, block>>>(reinterpret_cast<uint8_t*>(uv.ptr), uv.pitch, uv.width, uv.height, alpha);
}

} // namespace camera_driver_demo
