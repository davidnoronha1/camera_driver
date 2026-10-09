#pragma once

#include "camera_driver/publishers/nv_cuda_publisher.hpp"

namespace camera_driver_demo {

// Blends `frame`'s NV12 chroma (U/V) plane toward pure red's chroma value,
// in place, via a CUDA kernel running directly on the GPU memory backing the
// frame. alpha in [0,1]: 0 leaves the frame untouched, 1 makes every pixel
// fully red-tinted. Luma (Y) is left alone, so brightness/shape (e.g. the
// videotestsrc ball) still comes through under the tint.
//
// Exists purely to prove NvCUDAPublisher hands back live, writable device
// memory rather than a read-only copy: this demo takes `frame` by const&
// (the CudaFrame struct itself isn't modified) but writes through the
// device pointers it holds.
//
// No-op if frame isn't NV12, or if the chroma plane is a CUDA array
// (Jetson block-linear — see CudaPlane) rather than a linear device
// pointer.
void applyRedTint(const camera_driver::CudaFrame& frame, float alpha = 0.6f);

} // namespace camera_driver_demo
