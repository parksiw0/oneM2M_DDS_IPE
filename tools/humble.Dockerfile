# PC(Ubuntu 24.04)에서 Humble IPE 실행용 — host 네트워크로 DDS/localhost 공유.
# build:  docker build -f tools/humble.Dockerfile -t ipe:humble tools/
# run:    bash tools/run_ipe_humble.sh [--explain|--discover|...]
FROM ros:humble-ros-base

RUN apt-get update && apt-get install -y --no-install-recommends \
      ros-humble-rmw-cyclonedds-cpp \
      ros-humble-rosidl-runtime-py \
      ros-humble-sensor-msgs ros-humble-nav-msgs ros-humble-geometry-msgs \
      ros-humble-turtlebot3-msgs \
      python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir pyyaml requests cerberus

ENV RMW_IMPLEMENTATION=rmw_cyclonedds_cpp
