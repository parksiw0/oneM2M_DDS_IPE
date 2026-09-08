# Docker Compose로 IPE 실행하기

맥북에 ROS 2나 Python 패키지를 설치하지 않고 IPE를 실행한다. 기존
`python3 main.py` 실행도 그대로 사용할 수 있다. 기본 Compose는 **IPE를 실행**하고,
`live-sync` 프로필은 디자인 툴 알림용 Mosquitto 브로커를 추가한다.
TinyIoT와 디자인 툴은 별도로 실행하며, TinyIoT DB는 변경하지 않는다.

이미지에 ROS 2 Humble, Fast DDS/Cyclone DDS, TurtleBot3 메시지 패키지와 IPE가
포함된다. `platform`을 고정하지 않아 Apple Silicon은 ARM64, Intel은 AMD64
이미지를 사용한다. IPE 내부 상태는 `ipe-state` Docker 볼륨의 SQLite에 보관한다.
이는 TinyIoT가 리소스를 저장하는 PostgreSQL과 별개의 저장소다.

## 1. 맥북 네트워크 준비

Docker Desktop **4.34 이상**에서 다음 기능을 켠다.

1. Settings → Resources → Network → **Enable host networking**.
2. Apply and restart.
3. 맥북과 터틀봇을 같은 LAN에 연결하고, 로봇의 ROS bringup을 실행한다.

Docker Desktop의 host networking은 TCP/UDP를 지원하지만, Linux의 host network와
달리 맥북의 네트워크 인터페이스에 직접 접근하지 못한다. 따라서 이 설정만으로
**실로봇 DDS 발견·데이터 수신을 보장하지 않는다**. 아래 discover와 실제 메시지
수신을 맥북에서 반드시 먼저 확인한다. Compose를 추가해도 DDS 전송 구현은 동일하다.

공식 설명: [Docker host networking](https://docs.docker.com/engine/network/drivers/host/),
[Docker Desktop networking](https://docs.docker.com/desktop/features/networking/).

## 2. 접속 주소 설정

저장소 루트에서 한 번 실행한다.

```bash
cp .env.example .env
```

`.env`에서 배치에 맞는 주소를 설정한다.

| 위치 | `IPE_CSE_ENDPOINT` | `IPE_CSE_POA` |
|---|---|---|
| TinyIoT가 맥북에서 실행 | `http://127.0.0.1:3000` | `http://127.0.0.1:5050` |
| TinyIoT가 다른 Linux PC에서 실행 | `http://<LINUX_PC_IP>:3000` | `http://<MAC_LAN_IP>:5050` |

`ENDPOINT`는 IPE가 CSE에 접속하는 주소다. `POA`는 **CSE가 IPE에 알림을 보내는
주소**이므로, 원격 CSE 구성에서는 `127.0.0.1`을 쓰면 안 된다. 원격 CSE가 있는
PC에서 맥북의 TCP 5050에 접근 가능해야 구독 검증과 명령 알림이 동작한다.

TinyIoT 자체도 다른 컨테이너에 있다면 그 컨테이너에서 접근 가능한 주소를 POA로
지정한다. 예를 들어 Docker Desktop의 bridge 컨테이너에서는
`http://host.docker.internal:5050`을 사용할 수 있다.

`IPE_ROS_DOMAIN_ID`는 로봇과 같은 값으로 설정한다(현재 로봇: `30`).
`IPE_RMW_IMPLEMENTATION`이 비어 있으면 기존 IPE처럼 DDS vendor를 자동 선택한다.
현재 Fast DDS 로봇에 고정하려면 `rmw_fastrtps_cpp`를 지정한다.
`IPE_ROS_PEER`는 기존 Cyclone DDS용 옵션으로, Fast DDS나 Docker Desktop의 NAT
문제를 자동 해결하는 설정이 아니다.

토픽 매핑·QoS·리소스 이름·제어 정책은 기존 `config.py`에서 설정한다.
Compose에서는 `deploy/compose_config.py`가 여기에 `.env`의 접속 설정과 SQLite
상태 저장 경로를 적용한다. 원본 `config.py`의 PostgreSQL 설정은 수정하지 않는다.

## 3. 빌드와 DDS 연결 확인

```bash
docker compose build
docker compose run --rm -e IPE_RUN_MODE=discover ipe
```

`discover`는 ROS 인터페이스 목록만 확인하며 CSE 리소스를 생성하지 않는다.
로봇 토픽과 메시지 타입이 보여야 한다. 시작 시 기본 대기 시간은 30초이며,
토픽이 없으면 이유를 출력하고 종료한다. 자동 재시작은 설정하지 않았다.

목록이 보인 다음에는 로봇 메시지가 실제로 들어오는지도 확인한다.
아래는 현재 Fast DDS/도메인 30인 TurtleBot의 예다. IPE가 실행 중이지 않을 때 실행한다.

```bash
docker compose run --rm \
  -e ROS_DOMAIN_ID=30 -e RMW_IMPLEMENTATION=rmw_fastrtps_cpp \
  ipe timeout 15 ros2 topic echo /odom --once
```

`No ROS 2 endpoints` 또는 메시지 수신 타임아웃이면 도메인, RMW, bringup,
LAN 연결과 Docker Desktop 네트워크를 확인한다. 기존 `docker run --net=host`도
동일한 제약을 받는다. Desktop에서 DDS 경로가 확보되지 않으면 맥북의 LAN에
직접 연결되는 브리지 네트워크 Ubuntu VM에서 Docker Engine과 같은 Compose를
실행하는 구성을 대안으로 사용할 수 있다. 맥북에서 DDS 검증이 끝나기 전에는 본 시연에 사용하지 않는다.

## 4. 데모 실행

그래프 → 리소스 트리 → ROS 조작 → oneM2M 제어의 전체 순서와 명령은
[TurtleBot 데모 가이드](demo/README.md)에 있다. `.env.example`은 이 시연을 위해
`IPE_DEMO=1`로 설정돼 있다. 기존 전체 매핑을 사용하려면 `IPE_DEMO=0`으로 바꾼다.

로봇 bringup과 TinyIoT가 실행 중인 상태에서 저장소 루트에서 실행한다.

```bash
docker compose up
```

터미널을 계속 사용할 경우:

```bash
docker compose up -d
docker compose logs -f ipe
```

구조 생성과 데이터 전송을 나눠 보여주려면 IPE가 중지된 상태에서 먼저 실행한다.

```bash
docker compose run --rm -e IPE_RUN_MODE=bootstrap ipe
```

디자인 툴에서 구조를 확인한 후 `docker compose up`을 실행한다.
`bootstrap`은 리소스 생성 후 종료하고, `run`은 ROS 메시지를 계속 전달한다.
원격 CSE를 사용할 때 디자인 툴도 `http://<LINUX_PC_IP>:3000/TinyIoT`에 연결한다.

IPE가 실행되면 맥북에서 리스너와 상세 상태를 확인할 수 있다.

```bash
curl http://127.0.0.1:5050/healthz
curl http://127.0.0.1:5050/diag
```

`healthz`는 HTTP 리스너 도달성만 확인한다. DDS 연결과 샘플 전달은 IPE 로그 및
`diag`, 실제 CSE의 CIN 생성까지 확인한다. 시연 PC의 기존 IPE와 같은 포트 5050을
동시에 사용하지 않는다.

종료는 `Ctrl+C` 또는 다음 명령을 사용한다. 내부 상태 볼륨은 유지된다.

```bash
docker compose down
```

소스나 Dockerfile을 변경한 뒤에는 `docker compose up --build`로 이미지도 갱신한다.
`.env` 변경은 `docker compose up -d --force-recreate`로 적용한다.
`config.py`는 읽기 전용으로 마운트되므로 변경 후 `docker compose restart ipe`로
반영할 수 있다. `main.py`의 `USE_DOCKER` 값은 Compose 실행에는 영향을 주지 않는다.

## 범위

IPE와 디자인 툴의 리소스 요청은 HTTP, IPE가 받는 명령 알림도 HTTP를 사용한다.
디자인 툴 Live Sync만 TinyIoT → MQTT TCP 1883 → 브로커 → MQTT WebSocket 9001
경로를 사용한다. TinyIoT와 같은 호스트에서 선택적으로 브로커를 실행한다.

```bash
docker compose --profile live-sync up -d mqtt
docker compose logs --tail=30 mqtt
```

이미 같은 포트의 브로커가 실행 중이면 기존 브로커를 사용한다. 새 브로커는 기본적으로
localhost에만 공개된다. 원격 CSE 구성에서는 CSE 호스트의 `.env`에
`DEMO_MQTT_BIND_IP=<CSE_HOST_LAN_IP>`를 지정하고 브로커를 그 호스트에서 실행한다.
현재 디자인 툴은 CSE 주소의 호스트에서 MQTT 1883과 WebSocket 9001을 찾는다.

브로커를 켜는 것과 TinyIoT의 MQTT 지원을 활성화하는 것은 별개다. CSE 재빌드 설정과
Live Sync 버튼 순서는 [데모 가이드](demo/README.md#0-시연-전-준비)에 있다.
TinyIoT의 큰 HTTP 응답 잘림 문제는 아직 수정되지 않았으며 이 Compose가 해결하지 않는다.
