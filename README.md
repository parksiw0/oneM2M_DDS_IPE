# ROS 2 ↔ oneM2M IPE

ROS 2의 토픽·서비스·액션을 자동 발견하고 oneM2M CSE와 연결하는 브리지입니다.

**모든 실행 파라미터는 [config.py](config.py)에서 설정합니다.**

## 사용 기술

- Python 3.10+, ROS 2 Humble, DDS(Fast DDS / Cyclone DDS)
- oneM2M HTTP / MQTT, tinyIoT CSE
- PostgreSQL 또는 SQLite 상태 저장소
- Docker 실행 지원

## 구조

```text
config.py          실행 파라미터
main.py            네이티브 / Docker 실행
src/ipe/
├── adapter/       ROS 2 연동·메시지 변환
├── config/        설정 검증·인터페이스 매핑
├── core/          데이터 처리·QoS 정책
├── onem2m/        CSE 통신·리소스 관리
└── runtime/       자동 발견·전송 큐·워커·상태 저장
```

## 실행

1. ROS 2 로봇과 CSE를 실행합니다.
2. [config.py](config.py)에서 CSE 주소, DDS 도메인, DB를 설정합니다.
   기본 DB는 PostgreSQL이며, SQLite는 `CONFIG["storage"]["backend"] = "sqlite"`로 선택합니다.
3. 저장소 루트에서 실행합니다.

```bash
source /opt/ros/humble/setup.bash
python3 -m pip install --upgrade "pip>=24"
python3 -m pip install -e .
python3 main.py
```

Docker는 `USE_DOCKER = True`, MQTT 의존성은 `python3 -m pip install -e ".[mqtt]"`로 설정합니다.
