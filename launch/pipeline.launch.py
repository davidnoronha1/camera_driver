"""Run a YAML camera pipeline with ROS image publishers."""
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description():
    return LaunchDescription([
        DeclareLaunchArgument('pipeline_config', description='Absolute pipeline YAML path'),
        Node(package='camera_driver', executable='camera_driver_ros2_node',
             output='screen', parameters=[{
                 'pipeline_config': LaunchConfiguration('pipeline_config'),
             }]),
    ])
