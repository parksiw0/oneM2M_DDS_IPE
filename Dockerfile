# ROS 2 Humble runtime for hosts that do not provide Humble packages.
# Build with: docker build -t ipe:humble .
FROM ros:humble-ros-base

RUN apt-get update && apt-get install -y --no-install-recommends \
      ros-humble-rmw-fastrtps-cpp \
      ros-humble-rmw-cyclonedds-cpp \
      ros-humble-rosidl-runtime-py \
      ros-humble-rcl-interfaces ros-humble-std-msgs ros-humble-std-srvs \
      ros-humble-sensor-msgs ros-humble-nav-msgs ros-humble-geometry-msgs \
      ros-humble-tf2-msgs \
      ros-humble-turtlebot3-msgs \
      python3-tk \
      python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir requests cerberus "paho-mqtt>=2.1" \
      "psycopg[binary,pool]>=3.1,<4"
