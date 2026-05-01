import os
from ament_index_python.packages import get_package_share_directory
from launch import LaunchDescription
from launch_ros.actions import Node


def generate_launch_description():
    pkg_share = get_package_share_directory('camera_driver')
    video_path = os.path.join(pkg_share, 'resource', 'dock_vid.mkv')

    return LaunchDescription([
        Node(
            package='camera_driver',
            executable='camera_driver_exe',
            name='camera_driver',
            parameters=[{
                'image_source': 'file://' + video_path,
                'ros_topic': 'camera',
                'camera_frame_id': 'camera',
                'framerate': 30,
                'bitrate': 2000,
                'crop': '0,0,0.5,1',
                'port': 8554,
            }],
            output='screen',
        ),
    ])
