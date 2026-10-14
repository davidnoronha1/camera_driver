#!/usr/bin/env python3
"""Smoke-test a built ROS node against the synthetic camera pipeline.

Usage: python3 test/ros2_smoke.py /path/to/camera_driver_ros2_node
Requires a sourced ROS environment; no physical camera needed.
"""
import pathlib
import signal
import subprocess
import sys
import time

import rclpy
from rclpy.qos import qos_profile_sensor_data
from sensor_msgs.msg import Image


def main():
    executable = str(pathlib.Path(sys.argv[1]).resolve())
    config = pathlib.Path(__file__).resolve().parents[1] / 'config/ros2_test.yaml'
    # Invalid configuration must fail rather than appearing to run successfully.
    missing = subprocess.run([executable], timeout=15, capture_output=True)
    assert missing.returncode == 1, missing.stderr.decode()
    rclpy.init()
    node = rclpy.create_node('camera_driver_smoke')
    frames = []
    subscription = node.create_subscription(Image, '/camera/image', frames.append,
                                            qos_profile_sensor_data)
    process = subprocess.Popen([executable, '--ros-args', '-p', f'pipeline_config:={config}'])
    try:
        deadline = time.monotonic() + 15
        while len(frames) < 3 and time.monotonic() < deadline:
            assert process.poll() is None, 'Camera node exited before publishing frames'
            rclpy.spin_once(node, timeout_sec=0.2)
        assert len(frames) >= 3, 'No synthetic camera frames received'
        for frame in frames:
            assert (frame.width, frame.height, frame.encoding) == (320, 240, 'rgb8')
            assert frame.header.frame_id == 'camera_optical_frame'
            assert len(frame.data) == frame.height * frame.step
        process.send_signal(signal.SIGINT)
        assert process.wait(timeout=10) == 0, 'ROS shutdown failed'
        print('PASS: invalid config, YAML factories, ROS images, graceful shutdown')
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        node.destroy_subscription(subscription)
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
