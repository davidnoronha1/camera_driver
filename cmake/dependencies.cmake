include(FetchContent)

# ── MCAP ──────────────────────────────────────────────────────────────────
# Vendors the header-only MCAP C++ library (github.com/foxglove/mcap) plus
# lz4/zstd, exposing a plain `mcap` CMake target. Same fetch used by the
# sibling orbbec_iceoryx project.
FetchContent_Declare(
    mcap_builder
    GIT_REPOSITORY https://github.com/marc-medley/mcap_builder.git
    GIT_TAG        origin/platform_checks
)
FetchContent_MakeAvailable(mcap_builder)
