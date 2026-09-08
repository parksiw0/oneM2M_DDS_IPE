# TurtleBot 데모: ROS 그래프 → oneM2M 트리 → 관측 → 제어

먼저 [Compose 실행 준비](../DOCKER_COMPOSE.md)를 끝낸다. 실제 로봇 bringup을 켜고,
맥북과 로봇 사이의 DDS 발견 **및 `/odom` 메시지 수신**을 확인한다.

`.env`에 `IPE_DEMO=1`, `IPE_RUN_MODE=run`을 설정한다. 데모 설정은 **루트 namespace의
TurtleBot3 Humble**(`/cmd_vel`: `geometry_msgs/msg/Twist`)용이다. 다른 namespace나
`TwistStamped` 로봇이라면 `deploy/compose_config.py`와 조작 스크립트를 맞춰야 한다.

데모 설정에서 IPE가 연결하는 토픽은 `/odom`과 `/cmd_vel` 두 개다.

| 토픽 | oneM2M 표현 | 시연 포인트 |
|---|---|---|
| `/odom` | observe CNT, 1초 간격, 최근 CIN 5개 | 이동에 따라 `con.data.pose.pose.position` 변경 |
| `/cmd_vel` | observe CNT, 최근 CIN 5개 | ROS 조작 스크립트의 속도 메시지가 CIN으로 생성 |
| `/cmd_vel` | command CNT + SUB | 명령 CIN → CSE 알림 → IPE → ROS publish |

센서는 로봇이 정지한 동안에도 발행한다. “움직일 때만 토픽이 생성된다”고 설명하지
않고, **지정한 토픽만 연결하며 이동하면 값이 달라진다**고 설명한다. IPE의
`status`/QoS 관리 리소스도 갱신된다. `IPE_DEMO=0`이면 기존 config.py 매핑을 사용한다.

## 0. 시연 전 준비

이 가이드에서 로봇 조작은 **사용자가 이미 가지고 있는 ROS 조작 스크립트**를 쓴다.
실행 명령과 실행 위치는 기존에 사용하던 것을 그대로 따른다. 마지막 oneM2M 제어
단계에서는 그 스크립트를 종료하여 `/cmd_vel` 발행이 겹치지 않도록 한다.

통신 경로는 다음과 같다.

```text
로봇 ← ROS 2 DDS → IPE ← HTTP 요청/알림 → TinyIoT
디자인 툴 ── HTTP 조회·생성·수정·삭제 ──→ TinyIoT
TinyIoT ── MQTT TCP :1883 ──→ 브로커 ── MQTT WebSocket :9001 ──→ Live Sync
```

맥북에서 Docker Desktop의 host networking을 켠 후 저장소 루트에서 실행한다.
`.env`가 이미 있으면 복사하지 말고 해당 파일의 값을 확인한다.

```bash
cp .env.example .env
docker compose build ipe
```

TinyIoT가 맥북에서 실행 중인 경우 `.env`의 핵심 값은 다음과 같다.

```dotenv
IPE_DEMO=1
IPE_RUN_MODE=run
IPE_CSE_ENDPOINT=http://127.0.0.1:3000
IPE_CSE_POA=http://127.0.0.1:5050
IPE_ROS_DOMAIN_ID=30
```

TinyIoT가 다른 PC에 있으면 ENDPOINT를 그 PC 주소로, POA를 맥북의 LAN 주소로
바꾼다. TinyIoT와 디자인 툴은 별도 실행이다. 디자인 툴은 해당 저장소에서
`npm ci`(최초 준비), `npm run dev`로 시작하고 표시되는 주소를 브라우저로 연다.

Live Sync를 사용하려면 TinyIoT와 같은 호스트에서 브로커를 준비한다.
이미 1883/9001 브로커가 있으면 기존 것을 사용한다.

```bash
docker compose --profile live-sync up -d mqtt
```

현재 확인한 TinyIoT 소스에서는 MQTT가 꺼져 있다. 실제 사용할 CSE의
`source/server/config.h`에서 `ENABLE_MQTT`를 활성화하고,
`ENABLE_MQTT_WEBSOCKET`은 주석으로 둔다. `MQTT_HOST`는 브로커 주소로 설정한다.
해당 소스 디렉터리에서 `make clean`, `make server`로 재빌드한 뒤 기존 실행 방식으로
CSE를 재시작해야 한다. 브로커 실행만으로 CSE 설정이 바뀌지는 않는다.
현재 Linux 환경에서 확인했던 실행 바이너리는 `~/test/tinyIoT` 쪽이었으므로,
`~/workspace/tinyIoT` 수정만으로 실행 중인 서버에 반영됐다고 판단하지 않는다.

디자인 툴의 설정은 **Protocol Mode: HTTP**, **Originator: CAdmin**,
**CSE Platform: TinyIoT**로 맞춘다. 접속 주소는 로컬이면
`http://127.0.0.1:3000/TinyIoT`, 원격이면 `http://<CSE_PC_IP>:3000/TinyIoT`다.
원격 구성의 브로커 주소 설정은 [Compose 가이드](../DOCKER_COMPOSE.md#범위)를 따른다.

로봇 bringup을 시작하고 IPE가 중지된 상태에서 DDS 연결을 먼저 확인한다.

```bash
docker compose run --rm -e IPE_RUN_MODE=discover ipe
docker compose run --rm -e ROS_DOMAIN_ID=30 -e RMW_IMPLEMENTATION=rmw_fastrtps_cpp ipe timeout 15 ros2 topic echo /odom --once
```

도메인/RMW가 다른 로봇이면 두 번째 명령도 실제 설정에 맞춘다. 토픽 목록만 보이는
것으로 충분하지 않고 `/odom` 메시지를 받아야 한다. Mac의 실로봇 DDS 연결은 아직
검증하지 않았다. 처음의 `ERR_CONTENT_LENGTH_MISMATCH`도 아직 수정되지 않았으므로,
큰 HTTP 트리 조회가 실패한다면 전체 트리 시연 전에 CSE 응답 처리를 수정해야 한다.

## 1. 그래프를 먼저 보여주기

IPE 본 실행 전에:

```bash
docker compose build
docker compose run --rm ipe python3 /opt/ipe/demo/run.py graph
```

맥북에서 `artifacts/ros-graph.html`을 브라우저로 연다. 외부 서버나 CDN 없이 열리며,
실제 ROS publisher → topic → subscriber 연결을 보여준다. 매핑할 두 토픽을 청록색으로
표시하고, 아래 표에 생성 예정 oneM2M 경로를 표시한다. 이때 CSE에는 쓰지 않는다.
`artifacts/ros-graph.json`에는 같은 그래프 데이터를 저장한다.

발표: “로봇의 ROS 그래프를 발견했습니다. 여기서 `/odom`과 `/cmd_vel`을
oneM2M으로 연결하겠습니다.”

## 2. 리소스 트리를 생성하기

```bash
docker compose run --rm -e IPE_RUN_MODE=bootstrap ipe
```

디자인 툴에서 `CSE Load`와 `Resource Load`로 구조를 확인한다. 첫 시연 전에는
IPE를 중지하고 생성 전 화면도 보여준다. 이전 시연의 `ros2-ipe`가 남아 있으면
`bootstrap`은 기존 리소스를 재사용하므로 새로 생성되는 장면과는 다르다.

```text
TinyIoT/ros2-ipe/
├── robots/robot/topics/
│   ├── observe/odom/       ← 처음 생성한 경우 아직 센서 CIN 없음
│   ├── observe/cmd_vel/   ← 처음 생성한 경우 아직 속도 CIN 없음
│   └── command/cmd_vel/   ← IPE가 구독하는 명령 입력 CNT
├── status/commandStatus/
└── config/...
```

`bootstrap`은 구조와 SUB를 만든 후 종료한다. 구조를 로드한 뒤 **Live Sync ON**을
누른다. 현재 코드는 로드된 CNT/FCNT/TS들을 순회해서 구독을 만들므로 이 순서로
진행한다. 콘솔의 구독 통계에서 `errors: 0`인지 확인한다. ON 표시만으로 모든
컨테이너가 구독됐다고 판단하지 않는다. 이어서 데이터를 전달하는 IPE를 켠다.

```bash
docker compose up -d ipe
docker compose logs -f ipe
```

로그에서 `RUNNING`을 확인한다. 다른 터미널에서 필요한 데이터만 계속 조회한다.

```bash
docker compose run --rm ipe python3 /opt/ipe/demo/run.py watch
```

`watch`는 세 경로의 최신 CIN(`/la`)만 조회한다. 출력의 `ri`/`ct`와 `con`으로 새 CIN과
내용 변화를 확인한다. `cmd_vel`이나 `commandStatus`가 아직 없으면 404가 표시되며,
해당 경로에 첫 데이터가 생성되면 값이 출력된다. 전체 트리를 매번 가져오지 않는다.

디자인 툴 일반 트리는 CIN을 1개로 줄여 표시하므로 여러 CIN은 확대뷰에서 확인한다.
Live Sync를 쓰려면 TinyIoT/MQTT 연결이 별도로 준비돼 있어야 한다. 현재의 큰 HTTP
응답 잘림 문제가 남아 있으므로 계속된 전체 트리 재로드보다는 이 조회 화면을 사용한다.

## 3. ROS 조작 → 데이터 CIN 생성

로봇이 이동할 공간을 확보한 뒤 **기존 ROS 조작 스크립트를 기존 명령으로 실행한다**.
새 ROS 조작 스크립트는 필요 없다. IPE와 Live Sync를 켜 둔 상태에서
`observe/cmd_vel`의 속도와 `observe/odom`의 위치 변화를 보여준다.
이 매핑은 `/cmd_vel`이 `geometry_msgs/msg/Twist`, `/odom`이
`nav_msgs/msg/Odometry`인 구성이다. 기존 스크립트가 다른 토픽이나 namespace로
발행한다면 IPE 매핑도 그 경로에 맞춰야 한다.

발표: “ROS에서 보낸 속도 명령과 로봇의 주행 상태가 oneM2M 데이터로 전달됩니다.”

## 4. oneM2M 명령 → 로봇 제어

ROS 조작 스크립트가 종료된 뒤 다음 명령을 실행한다.

```bash
docker compose run --rm ipe python3 /opt/ipe/demo/run.py cse-drive --angular 0.2 --seconds 2
```

이 스크립트는 ROS publisher를 만들지 않는다. HTTP로 다음 **command CNT에 직접 CIN**을
생성한다. `publishRequest`라는 하위 CNT를 추가하지 않는다.

```text
POST /TinyIoT/ros2-ipe/robots/robot/topics/command/cmd_vel
Content-Type: application/json;ty=4
X-M2M-Origin: CAdmin
```

`con`은 다음 객체를 JSON 문자열로 인코딩한다. 각 전송의 `commandId`는 다르다.

```json
{
  "commandId": "demo-<unique-id>",
  "linear": {"x": 0.0, "y": 0.0, "z": 0.0},
  "angular": {"x": 0.0, "y": 0.0, "z": 0.2}
}
```

화면에서 순서대로 보여준다.

1. `command/cmd_vel`에 생성한 CIN.
2. `status/commandStatus`에서 같은 `commandId`의 `event: published`.
3. 실제 로봇 회전과 `/odom` 변화.

HTTP 성공은 CSE 저장 성공이다. `published`는 IPE의 ROS 발행 성공이며 실제 로봇
동작 완료 응답은 아니다. 따라서 로봇과 odometry도 함께 확인한다. IPE는 자기 에코를
억제하므로 자기 명령이 `observe/cmd_vel`에 다시 나타나는 것을 성공 기준으로 삼지 않는다.

`IPE_DEMO=1`은 `/cmd_vel`을 명시적으로 제어 허용하며 첫 명령 GUI 승인에 의존하지
않는다. 기존 `OBSERVE_ONLY=True`를 설정했다면 제어는 계속 비활성화된다.

## 5. 정지와 종료

이 저장소의 `cse-drive`는 지정 시간이 끝나거나 `Ctrl+C`/SIGTERM을 받으면 0 속도를
3번 전송한다. 기존 ROS 조작 스크립트의 정지 동작은 그 스크립트의 사용법을 따른다.
IPE 경유 명령에는 1초 watchdog과 2초 신선도 검사를 설정했고, 속도 범위는
선속도 ±0.1m/s, 각속도 ±0.5rad/s, 스크립트 실행 시간은 최대 5초다.

oneM2M 경로로 정지 명령만 보내려면:

```bash
docker compose run --rm ipe python3 /opt/ipe/demo/run.py stop
```

CSE 경로가 끊겼지만 ROS가 연결돼 있다면:

```bash
docker compose run --rm ipe python3 /opt/ipe/demo/run.py ros-drive --seconds 0
```

정지 전송은 통신이 살아 있을 때만 가능하다. 원격 경로가 모두 끊겼거나 프로세스가
강제 종료된 경우에는 로봇 측에서 정지해야 한다. 서로 다른 조작 스크립트를 동시에
실행하지 않는다. 마지막으로 `docker compose down`으로 IPE를 종료한다.
