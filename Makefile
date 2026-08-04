COMPOSE  ?= docker compose
SERVICE  ?= camera_driver_nogpu
WORKDIR  := /workspace/camera_driver
EXE      := /usr/local/bin/camera_driver_exe

DEMO_PIPELINE := config/debug_display.yaml
PIPELINE      ?= config/example_pipeline.yaml

.PHONY: pull-docker pull-docker-nogpu build-docker build-docker-nogpu run-demo-pipeline run-pipeline

# Pull the image published by GitHub Actions (ghcr.io); build locally only if
# it hasn't been published yet. This is the preferred way to get an image.
pull-docker:
	$(COMPOSE) pull camera_driver || $(COMPOSE) build camera_driver

# CPU-only variant of the above.
pull-docker-nogpu:
	$(COMPOSE) pull camera_driver_nogpu || $(COMPOSE) build camera_driver_nogpu

# Explicit local GPU build (overrides the published image until a re-pull).
# NVIDIA DeepStream base image; requires nvidia-container-toolkit at runtime.
build-docker:
	$(COMPOSE) build camera_driver

# Explicit local CPU-only build, no NVIDIA GPU/driver required.
build-docker-nogpu:
	$(COMPOSE) build camera_driver_nogpu

# Runs config/debug_display.yaml (MJPEG HTTP stream on :8080 + on-screen
# debug window). Override SERVICE=camera_driver to run against the GPU build.
run-demo-pipeline:
	$(COMPOSE) run --rm $(SERVICE) $(EXE) $(WORKDIR)/$(DEMO_PIPELINE)

# make run-pipeline PIPELINE=config/example_pipeline.yaml
run-pipeline:
	$(COMPOSE) run --rm $(SERVICE) $(EXE) $(WORKDIR)/$(PIPELINE)
