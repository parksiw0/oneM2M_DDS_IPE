# TurtleBot3 Burger 토픽·QoS 카탈로그 (Humble)

토픽별 QoS를 적어둔 공식 문서는 없다 — e-Manual은 토픽 이름까지만 다루고, QoS는 각 노드의 소스에만 있다. 이 문서는 소스 원문을 직접 확인해(2026-07-15) 한곳에 모은 것이다. `config/profiles/turtlebot3.yaml`의 요청 QoS와 짝을 이룬다.

표기: R = RELIABLE, BE = BEST_EFFORT, V = VOLATILE, TL = TRANSIENT_LOCAL, KL(n) = KEEP_LAST depth n.

## 로봇 발행 (observe 대상 10개)

| 토픽 | 타입 | 발행 노드 | offered QoS | 근거 | 프로필 요청 |
|---|---|---|---|---|---|
| /scan | sensor_msgs/LaserScan | ld08_driver | **BE** · V · SensorDataQoS(KL 5) | [ld08 main.cpp] | sensor_stream (BE·KL5) + 0.5 Hz |
| /odom | nav_msgs/Odometry | turtlebot3_node | R · V · KL(10) | [odometry.cpp] | sensor_stream+depth10 + 2 Hz |
| /imu | sensor_msgs/Imu | turtlebot3_node | R · V · KL(10) | [sensors.hpp] | sensor_stream + 1 Hz |
| /magnetic_field | sensor_msgs/MagneticField | turtlebot3_node | R · V · KL(10) | [sensors.hpp] | sensor_stream + 1 Hz |
| /joint_states | sensor_msgs/JointState | turtlebot3_node | R · V · KL(10) | [sensors.hpp] | sensor_stream + 1 Hz |
| /sensor_state | turtlebot3_msgs/SensorState | turtlebot3_node | R · V · KL(10) | [sensors.hpp] | sensor_stream + 1 Hz |
| /battery_state | sensor_msgs/BatteryState | turtlebot3_node | R · V · KL(10) | [sensors.hpp] | state_reliable (R·KL10) + 0.2 Hz |
| /tf | tf2_msgs/TFMessage | odometry + robot_state_publisher | R · V · KL(100) | [tf2_ros qos.hpp]* | sensor_stream + 1 Hz |
| /tf_static | tf2_msgs/TFMessage | robot_state_publisher | R · **TL** · KL(1), 1회 발행 | [tf2_ros qos.hpp]* | latched_state (R·TL·KL1) |
| /robot_description | std_msgs/String | robot_state_publisher | R · **TL**, 1회 발행 | [robot_state_publisher]* | latched_state |

주의: turtlebot3_node 발행자는 SensorDataQoS가 아니라 전부 기본 RELIABLE(KL 10)이다 — BE인 것은 /scan 하나뿐. IPE의 BE 요청은 최약-호환 조정이라 R 발행자와도 매칭되며, 실측 offered는 각 토픽 `qos` FCNT의 peers 속성에 기록된다.

## 로봇 구독 (command 대상 4개)

| 토픽 | 타입 | 로봇 requested QoS | 근거 | 프로필 발행 |
|---|---|---|---|---|
| /cmd_vel | geometry_msgs/Twist | R · V · KL(10) | [turtlebot3.cpp] | cmd_reliable + 클램프·워치독 500ms |
| /motor_power | std_msgs/Bool | R · V · KL(10) | [turtlebot3.cpp] | cmd_reliable |
| /sound | turtlebot3_msgs/Sound | R · V · KL(10) | [turtlebot3.cpp] | cmd_reliable |
| /reset | std_msgs/Empty | R · V · KL(10) | [turtlebot3.cpp] | cmd_reliable |

## 브리지 제외 2개

| 토픽 | 이유 |
|---|---|
| /rosout | 모든 노드(IPE 포함)가 발행하는 로그 — 자기 관측 루프 |
| /parameter_events | ROS 파라미터 인프라 |

## 근거 원문

소스(2026-07-15 원문 확인, humble 브랜치):

- [ld08 main.cpp] github.com/ROBOTIS-GIT/ld08_driver/blob/main/src/main.cpp — `create_publisher<LaserScan>("scan", rclcpp::QoS(rclcpp::SensorDataQoS()))`
- [sensors.hpp] github.com/ROBOTIS-GIT/turtlebot3/blob/humble/turtlebot3_node/include/turtlebot3_node/sensors/sensors.hpp — 센서 공통 `rclcpp::QoS(rclcpp::KeepLast(10))`
- [odometry.cpp] github.com/ROBOTIS-GIT/turtlebot3/blob/humble/turtlebot3_node/src/odometry.cpp — `create_publisher<Odometry>("odom", qos)`, qos = KeepLast(10)
- [turtlebot3.cpp] github.com/ROBOTIS-GIT/turtlebot3/blob/humble/turtlebot3_node/src/turtlebot3.cpp — cmd_vel 등 구독 `rclcpp::QoS(rclcpp::KeepLast(10))`

표준 문서(수치 정본; \* 표시는 표준 소스 기준, 실기 peers로 재확인 예정):

- docs.ros.org/en/humble/Concepts/Intermediate/About-Quality-of-Service-Settings.html — QoS 8정책·RxO 규칙
- github.com/ros2/rmw/blob/humble/rmw/include/rmw/qos_profiles.h — sensor_data(BE·5)/default(R·10) 수치
- [tf2_ros qos.hpp] github.com/ros2/geometry2/blob/humble/tf2_ros/include/tf2_ros/qos.hpp — Dynamic/StaticBroadcasterQoS
- [robot_state_publisher] github.com/ros/robot_state_publisher/blob/humble/src/robot_state_publisher.cpp — robot_description TL 발행
- emanual.robotis.com/docs/en/platform/turtlebot3/bringup/ — bringup 절차, ROS_DOMAIN_ID 관례

리포 내부: `QoS_FCNT_설계서.md` 2장·부록 F — QoS 정책 카탈로그의 rclpy/rmw 원문 대조 검증.
