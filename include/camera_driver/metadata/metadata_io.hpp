#pragma once

#include "camera_metadata.hpp"
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace camera_driver::metadata_io {

// Writes an already-JPEG-encoded image to `path` with `metadata` embedded
// (via jifmux + GST_TAG_COMMENT — mirrors how MkvRecorderElement embeds
// metadata into .mkv files). Runs a small one-shot pipeline synchronously;
// throws std::runtime_error on failure.
void writeImageWithMetadata(const std::vector<uint8_t>& jpeg_bytes,
                             const std::string& path,
                             const CameraMetadata& metadata);

// Reads CameraMetadata back from an .mkv (written by MkvRecorderElement) or
// .jpg/.jpeg (written by writeImageWithMetadata) file, dispatched by
// extension. Returns std::nullopt if the file has no camera-metadata
// comment tag. Deliberately does not use GstDiscoverer — it autoplugs a
// decode step that can engage hardware decoders that are unavailable or
// erroring in some environments (verified during design); this instead
// runs a minimal, non-decoding demux/parse pipeline that only reads
// container/file-level tags. Throws std::runtime_error on pipeline errors.
std::optional<CameraMetadata> readMetadataFromFile(const std::string& path);

} // namespace camera_driver::metadata_io
