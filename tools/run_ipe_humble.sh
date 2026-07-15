#!/usr/bin/env bash
# Humble 컨테이너에서 IPE 실행. 예:
#   bash tools/run_ipe_humble.sh --explain
#   bash tools/run_ipe_humble.sh --discover
#   bash tools/run_ipe_humble.sh              # 브리징 시작
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMG=ipe:humble

docker image inspect "$IMG" >/dev/null 2>&1 || \
  docker build -f "$ROOT/tools/humble.Dockerfile" -t "$IMG" "$ROOT/tools"

# --net=host: DDS 멀티캐스트·localhost:3000(tinyIoT)·:5050(POA)을 호스트와 공유
exec docker run --rm -it --net=host --ipc=host \
  -v "$ROOT":/ws -w /ws \
  -e ROS_DOMAIN_ID="${ROS_DOMAIN_ID:-30}" \
  -e RMW_IMPLEMENTATION=rmw_cyclonedds_cpp \
  -e CYCLONEDDS_URI="${CYCLONEDDS_URI:-}" \
  -e PYTHONPATH=/ws/src \
  "$IMG" bash -lc "source /opt/ros/humble/setup.bash && python3 -m ipe --config config/profiles/turtlebot3.yaml $*"
