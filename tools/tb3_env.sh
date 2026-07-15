# 로봇(버거, Ubuntu 22.04 + Humble) ~/.bashrc용 — PC는 tools/run_ipe_humble.sh가 대신함.
source /opt/ros/humble/setup.bash

export ROS_DOMAIN_ID=30
export RMW_IMPLEMENTATION=rmw_cyclonedds_cpp   # ros-humble-rmw-cyclonedds-cpp 설치 필요
export TURTLEBOT3_MODEL=burger
export LDS_MODEL=LDS-02                        # 구형 라이다면 LDS-01

# 공유기가 멀티캐스트를 막아 상대가 안 보이면 주석 해제 + 양쪽 IP 기입
# export CYCLONEDDS_URI='<CycloneDDS><Domain><Discovery><Peers>
#   <Peer address="192.168.219.PC_IP"/><Peer address="192.168.219.ROBOT_IP"/>
# </Peers></Discovery></Domain></CycloneDDS>'
