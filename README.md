# ROS 2 ↔ oneM2M IPE

ROS 2의 토픽·서비스·액션을 자동 발견하고 oneM2M CSE와 연결하는 브리지입니다.

**기본 실행 파라미터와 매핑 정책은 [config.py](config.py)에서 설정합니다.**
Compose의 접속 주소와 데모 모드는 [.env.example](.env.example)을 복사한 `.env`에서 설정합니다.

## 사용 기술

- Python 3.10+, ROS 2 Humble, DDS(Fast DDS / Cyclone DDS)
- oneM2M HTTP / MQTT, tinyIoT CSE
- PostgreSQL 또는 SQLite 상태 저장소
- Docker 실행 지원

## 구조

```text
config.py          실행 파라미터
main.py            네이티브 / Docker 실행
Dockerfile         Docker 이미지 빌드
src/ipe/
├── __init__.py    패키지 정보
├── __main__.py    python -m ipe 진입점
├── adapter/       ROS 2 연동·메시지 변환·RMW 선택
├── qos/           QoS별 조정·oneM2M 매핑·요청 해석
├── core/          공통 인터페이스 자료형·데이터 파이프라인·명령·트랜잭션
├── onem2m/        CSE 통신·리소스 관리
└── runtime/       앱 실행·수신·송신·연결 관리·상태 저장
```

설정 전용 `src/ipe/config/` 디렉터리는 사용하지 않습니다. 사용자가 바꾸는 값은
루트 `config.py`에 두고, `runtime/settings.py`가 전체 설정을 검증합니다.
`runtime/planning.py`는 발견한 인터페이스와 설정을 합쳐 연결 계획을 만들고,
`runtime/naming.py`는 로봇 식별과 경로를 계산합니다. QoS 검증 규칙은
`qos/configuration.py`, 전송 설정 규칙은 `onem2m/client.py`에서 제공합니다.

`runtime/cli.py`가 설정·실행 모드를 선택하고, `runtime/app.py`가 실행을 조립합니다.
`inbound.py`는 요청 처리, `outbound.py`는 데이터 송신·재시도,
`bindings.py`는 자동 발견·연결 변경, `status.py`는 QoS 상태 게시를 담당합니다.

공통 토픽·서비스·액션 자료형과 토픽 전달 형식(`TopicIR`)은 `core/models.py`에 둡니다.
QoS 자료형과 상태 전달 형식은 `qos/models.py`, CSE·MQTT 접속 자료형은
`onem2m/client.py`에서 관리합니다. 로봇 식별 자료형은 `runtime/naming.py`,
최종 연결 계획(`ResolvedConfig`)은 `runtime/planning.py`에 정의합니다.

복구 작업은 종류별로 병합하고, 실패한 프로비저닝 결과는 기존 경로에 적용하지 않습니다.
종료된 서비스·액션 요청은 늦은 응답으로 다시 변경하지 않습니다. 상태 DB의 종료 기록은
`recovery.dedup_retention_days`에 따라 `cleanup_interval_sec` 간격으로 정리합니다.

`qos/`는 `core/`와 같은 단계에 있습니다. `reliability.py`, `durability.py`, `history.py`,
`deadline.py`, `lifespan.py`, `liveliness.py`가 정책별 판단과 매핑을 맡습니다.
`engine.py`는 이 판단들을 조합하고, `registry.py`는 DDS 정책 22종의 지원 상태,
`codec.py`는 관리 리소스 표현, `requests.py`는 변경 요청을 처리합니다.
실제 ROS QoS 객체 생성은 `adapter/qos.py`, CSE 쓰기는 `runtime`에서 실행합니다.

## QoS 설정과 적용 범위

- `CONFIG["qos_profiles"]`: 기본 프로파일 값. observe에서 `qos`를 생략하면
  `sensor_data`를 기준으로 publisher QoS에 맞춰 자동 조정합니다.
- `CONFIG["defaults"]`: 토픽·서비스·액션 기본값. `topic_command.qos`는 양방향
  토픽의 command 기본값에도 적용됩니다. `bridge`의 개별 규칙으로 재정의할 수 있습니다.
- `CONFIG["policy"]`: QoS 엄격성, `KEEP_ALL`의 CSE 보관 상한, 신선도 기준,
  자기 에코 억제 및 QoS 이벤트 묶음 간격을 설정합니다.
- `CONFIG["qos_fcnt"]`: 관리 리소스 형식, 상태 게시 간격, 외부 변경 허용 여부입니다.

활성 ROS 2 RMW에서 읽고 설정하는 정책은 위 6종입니다. 나머지 정책은 관측할 수 없는
값을 임의로 채우지 않고 `UNAVAILABLE`로 표시합니다. DDS와 oneM2M의 의미가 완전히
같지는 않습니다. HISTORY의 `KEEP_ALL`도 CSE에서는 설정한 개수로 제한되며,
LIFESPAN은 IPE 수신 시각을 기준으로 CIN 만료 시각에 매핑합니다. 데이터 FCNT에는
샘플별 만료를 적용할 수 없어 CIN과 FCNT를 함께 쓰면 부분 적용으로 표시합니다.

## 실행

1. ROS 2 로봇과 CSE를 실행합니다.
2. [config.py](config.py)에서 CSE 주소, DDS 도메인, DB를 설정합니다.
   기본 DB는 PostgreSQL이며, SQLite는 `CONFIG["storage"]["backend"] = "sqlite"`로 선택합니다.
3. 저장소 루트에서 실행합니다.

```bash
source /opt/ros/humble/setup.bash
python3 -m venv --system-site-packages .venv
source .venv/bin/activate
python3 -m pip install --upgrade "pip>=24"
python3 -m pip install -e .
python3 main.py
```

가상환경 생성에 필요한 시스템 패키지는 `python3-venv`입니다. 이미 생성한 `.venv`는
활성화해서 재사용합니다. TurtleBot3 인터페이스 전체를 연결하려면 PC에도
`ros-humble-turtlebot3-msgs`가 필요하며, `discovery.domain_id`는 로봇과 같아야 합니다.

Docker는 `USE_DOCKER = True`, MQTT 의존성은 `python3 -m pip install -e ".[mqtt]"`로 설정합니다.

맥북에서 Compose로 실행하는 경우에는 [Docker Compose 실행 가이드](DOCKER_COMPOSE.md)를
따릅니다. Docker Desktop의 host networking을 켜고 `.env`에 CSE 접속 주소와 알림
수신 주소를 설정한 뒤 실행합니다. Compose는 IPE 내부 상태를 별도 SQLite 볼륨에
저장하며, TinyIoT와 디자인 툴은 별도로 실행합니다.
그래프 확인부터 양방향 로봇 제어까지의 시연 순서는 [TurtleBot 데모 가이드](demo/README.md)에 있습니다.

```bash
cp .env.example .env
# .env의 접속 주소 설정 후, 로봇 bringup과 TinyIoT를 실행합니다.
docker compose up --build
```

실로봇 DDS 연결은 맥북에서 사전 확인이 필요합니다. 기존 `python3 main.py`의
네이티브/Docker 실행 방식과 `config.py`의 매핑 설정도 계속 사용할 수 있습니다.

## 검증

```bash
python3 -m pip install -e ".[dev]"
ruff check --no-cache main.py src config.py
mypy main.py src
```

실제 DDS 연결과 CSE 통합 실행은 별도로 확인해야 합니다.
