#!/usr/bin/env bash
# 로봇(버거, Ubuntu 22.04 + Humble)에서 실행 — PC에서: ssh <user>@<robot-ip> 'bash -s' < tools/setup_robot_humble.sh
set -euo pipefail

echo "== ROS 2 Humble 확인"
[ -f /opt/ros/humble/setup.bash ] || { echo "[실패] Humble 미설치"; exit 1; }

echo "== cyclonedds RMW + TB3 패키지"
sudo apt-get update -qq
sudo apt-get install -y ros-humble-rmw-cyclonedds-cpp
dpkg -l | grep -q ros-humble-turtlebot3-bringup \
  || sudo apt-get install -y ros-humble-turtlebot3 ros-humble-turtlebot3-msgs

echo "== 환경 변수 (~/.bashrc)"
grep -q "ROS_DOMAIN_ID=30" ~/.bashrc || cat >> ~/.bashrc <<'ENV'

# --- TurtleBot3 x oneM2M IPE demo ---
source /opt/ros/humble/setup.bash
export ROS_DOMAIN_ID=30
export RMW_IMPLEMENTATION=rmw_cyclonedds_cpp
export TURTLEBOT3_MODEL=burger
export LDS_MODEL=LDS-02
ENV

echo "== OpenCR 포트 확인"
ls -l /dev/ttyACM0 2>/dev/null || echo "[주의] /dev/ttyACM0 없음 — OpenCR USB 연결 확인"
ls -l /dev/ttyUSB0 2>/dev/null || true   # LDS

echo "완료. 재로그인 후: ros2 launch turtlebot3_bringup robot.launch.py"
