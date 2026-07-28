#!/usr/bin/env bash
# Humble 컨테이너에서 IPE 실행. 예:
#   bash tools/run_ipe_humble.sh --explain
#   bash tools/run_ipe_humble.sh --discover
#   bash tools/run_ipe_humble.sh              # 브리징 시작
#   IPE_CONFIG=config/profiles/turtlebot3_motion_demo.yaml bash tools/run_ipe_humble.sh
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMG=ipe:humble
IPE_CONFIG="${IPE_CONFIG:-config/profiles/turtlebot3.yaml}"
IPE_TZ="${TZ:-}"
if [[ -z "$IPE_TZ" && -r /etc/timezone ]]; then
  read -r IPE_TZ </etc/timezone
fi
IPE_TZ="${IPE_TZ:-UTC}"

if [[ "$IPE_CONFIG" == /* || ! -f "$ROOT/$IPE_CONFIG" ]]; then
  printf 'IPE_CONFIG must be an existing path relative to %s: %s\n' "$ROOT" "$IPE_CONFIG" >&2
  exit 2
fi

docker image inspect "$IMG" >/dev/null 2>&1 || \
  docker build -f "$ROOT/tools/humble.Dockerfile" -t "$IMG" "$ROOT/tools"

docker_tty=()
if [[ -t 0 && -t 1 ]]; then
  docker_tty=(-it)
fi

# --net=host: DDS 멀티캐스트·localhost:3000(tinyIoT)·:5050(POA)을 호스트와 공유
exec docker run --rm "${docker_tty[@]}" --net=host --ipc=host \
  -v "$ROOT":/ws -w /ws \
  -e ROS_DOMAIN_ID="${ROS_DOMAIN_ID:-30}" \
  -e RMW_IMPLEMENTATION="${RMW_IMPLEMENTATION:-rmw_cyclonedds_cpp}" \
  -e CYCLONEDDS_URI="${CYCLONEDDS_URI:-}" \
  -e TZ="$IPE_TZ" \
  -e IPE_CONFIG="$IPE_CONFIG" \
  -e PYTHONPATH=/ws/src \
  "$IMG" bash -lc 'source /opt/ros/humble/setup.bash && exec python3 -m ipe --config "$IPE_CONFIG" "$@"' bash "$@"
