# Future work

- **ROS2-broker / inference-result sink** — nothing consumes `nvinferserver`
  output and republishes it to ROS2; would need a `nvmsgbroker` wrapper or a
  custom consumer reading inference metadata off buffer probes via `rclcpp`.
