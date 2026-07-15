#!/usr/bin/env bash
# qos FCNT 실기 검증(읽기 전용). tinyIoT + IPE(turtlebot3.yaml)가 떠 있는 상태에서 실행.
#   bash tools/verify_qos_fcnt.sh
CSE=${CSE:-http://localhost:3000}
BASE=${BASE:-TinyIoT}
AE=${AE:-tb3-ipe}
ORIGIN=${ORIGIN:-CAdmin}

req() {
  curl -fsS -H "X-M2M-Origin: $ORIGIN" -H "X-M2M-RVI: 3" \
       -H "X-M2M-RI: vfy-$RANDOM" -H "Accept: application/json" "$1"
}

pick() { python3 -c '
import json, sys
d = json.load(sys.stdin)
r = next(iter(d.values()))
keys = ("dir","iface","robot","rtype","pfRef","sver",
        "cfRlb","cfDrb","cfHst","cfDpt","cfDdl","cfLsp","cfLiv","cfLse",
        "apRlb","apDrb","apHst","apDpt","apDdl","apLsp","apLiv","apLse",
        "smode","evts","pcnt","dvAxs")
missing_ap = not any(k in r for k in ("apRlb",))
print("   ", {k: r[k] for k in keys if k in r})
if missing_ap: print("    [주의] ap* 없음 — 아직 바인딩 전이거나 게시 전")
'; }

echo "== 1) CSE 생존"
req "$CSE/$BASE" > /dev/null && echo "    OK"

echo "== 2) observe 토픽별 qos FCNT (cf*/ap*/메타)"
for t in scan odom imu joint_states battery_state; do
  echo "  - ros2Data/tb3/$t/qos"
  req "$CSE/$BASE/$AE/ros2Data/tb3/$t/qos" | pick \
    || echo "    [실패] 없음 — .fcp 미배포(lbl-only 폴백) 또는 IPE 미기동"
done

echo "== 3) command 방향 qos FCNT"
echo "  - ros2Command/tb3/cmd_vel/qos"
req "$CSE/$BASE/$AE/ros2Command/tb3/cmd_vel/qos" | pick \
  || echo "    [실패] 없음"

echo "== 4) lbl 병행(qos:* + ipe:qosResource 포인터)"
req "$CSE/$BASE/$AE/ros2Data/tb3/odom" | python3 -c '
import json, sys
d = json.load(sys.stdin)
lbl = next(iter(d.values())).get("lbl") or []
qos = [x for x in lbl if x.startswith(("qos:", "ipe:qosResource="))]
print("   ", qos if qos else "[주의] qos 라벨 없음 — 기동 게시 전이거나 lbl_compat off")
'
echo "== 완료"
