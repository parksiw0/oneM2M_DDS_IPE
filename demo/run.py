"""Graph export and bounded ROS/oneM2M TurtleBot demonstration commands."""

from __future__ import annotations

import argparse
import html
import json
import math
import os
import signal
import time
import uuid
from pathlib import Path


def velocity(linear: float, angular: float) -> dict:
    return {"linear": {"x": linear, "y": 0.0, "z": 0.0},
            "angular": {"x": 0.0, "y": 0.0, "z": angular}}


def run_motion(send, linear: float, angular: float, seconds: float) -> None:
    """Publish at 5 Hz; always attempt three zero-velocity commands on exit."""
    if not all(math.isfinite(x) for x in (linear, angular, seconds)):
        raise ValueError("Velocity and duration must be finite.")
    if abs(linear) > 0.1 or abs(angular) > 0.5 or not 0 <= seconds <= 5:
        raise ValueError("Demo limits: |linear| <= 0.1 m/s, |angular| <= 0.5 rad/s, 0 <= seconds <= 5.")
    deadline = time.monotonic() + seconds
    try:
        while time.monotonic() < deadline:
            send(velocity(linear, angular))
            time.sleep(min(0.2, max(0.0, deadline - time.monotonic())))
    finally:
        stopped = False
        for _ in range(3):
            try:
                send(velocity(0.0, 0.0))
                stopped = True
            except Exception as exc:
                print(f"Stop delivery failed: {exc}", flush=True)
            time.sleep(0.2)
        if not stopped:
            raise RuntimeError("No stop command could be delivered. Stop the robot directly.")


def runtime():
    import config
    from ipe.runtime.planning import resolve
    from ipe.runtime.settings import load_config

    os.environ["TZ"] = config.TIMEZONE
    time.tzset()
    return resolve(load_config(), discovered=None)


def ros_node(rc, name):
    import rclpy
    from rclpy.node import Node
    from rclpy.signals import SignalHandlerOptions

    from ipe.runtime.cli import _configure_ros_environment

    _configure_ros_environment(rc)
    # Keep main()'s handlers: shutting the ROS context down on SIGTERM would
    # prevent the finally block from publishing the stop messages.
    rclpy.init(signal_handler_options=SignalHandlerOptions.NO)
    return Node(name, enable_rosout=False, start_parameter_services=False)


def graph_html(topics: list[dict]) -> str:
    """Render actual publisher/topic/subscriber edges without external assets."""
    publishers = sorted({n for t in topics for n in t["publishers"]})
    subscribers = sorted({n for t in topics for n in t["subscribers"]})
    columns = [publishers, [t["name"] for t in topics], subscribers]
    height = max(200, 80 + 55 * max((len(c) for c in columns), default=0))
    positions = [{name: 70 + 55 * i for i, name in enumerate(c)} for c in columns]
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1200 {height}">',
             '<defs><marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto"><path d="M0,0 L0,6 L7,3z" fill="#93a4bd"/></marker></defs>']
    for label, x in zip(["Publishers", "ROS topics", "Subscribers"], [170, 600, 1030], strict=True):
        parts.append(f'<text x="{x}" y="25" text-anchor="middle" fill="#94a3b8">{label}</text>')
    for topic in topics:
        y = positions[1][topic["name"]]
        for peer in topic["publishers"]:
            start = positions[0][peer]
            parts.append(f'<path d="M330,{start} C380,{start} 390,{y} 440,{y}" fill="none" stroke="#93a4bd" marker-end="url(#arrow)"/>')
        for peer in topic["subscribers"]:
            end = positions[2][peer]
            parts.append(f'<path d="M760,{y} C810,{y} 820,{end} 870,{end}" fill="none" stroke="#93a4bd" marker-end="url(#arrow)"/>')
    mapped = {t["name"] for t in topics if t["paths"]}
    for column, x in enumerate([10, 440, 870]):
        for name, y in positions[column].items():
            color = "#155e75" if column == 1 and name in mapped else "#1e293b"
            label = html.escape(name)
            parts.append(f'<rect x="{x}" y="{y - 17}" width="320" height="34" rx="8" fill="{color}"/>')
            parts.append(f'<text x="{x + 160}" y="{y + 5}" text-anchor="middle" fill="#f8fafc" font-size="13"><title>{label}</title>{label}</text>')
    parts.append('</svg>')
    rows = []
    for topic in topics:
        cells = [html.escape(topic["name"]), html.escape(", ".join(topic["types"])),
                 "<br>".join(html.escape(p) for p in topic["paths"]) or "—"]
        rows.append('<tr>' + ''.join(f'<td>{c}</td>' for c in cells) + '</tr>')
    return ('<!doctype html><html lang="ko"><meta charset="utf-8">'
            '<title>ROS graph → oneM2M mapping</title><style>'
            'body{background:#0f172a;color:#e2e8f0;font:16px system-ui;margin:32px}'
            'svg{width:100%;min-width:1000px}section{overflow:auto}'
            'table{width:100%;border-collapse:collapse}th,td{padding:12px;text-align:left;'
            'border-bottom:1px solid #334155;overflow-wrap:anywhere}'
            '</style><h1>ROS graph → oneM2M mapping</h1>'
            '<p>실제로 발견한 ROS 그래프의 스냅숏입니다. 청록색 토픽은 IPE 매핑 대상입니다. '
            '아래 경로는 생성 예정 경로이며, 이 명령은 CSE 리소스를 생성하지 않습니다.</p>'
            '<section>' + ''.join(parts) + '</section><h2>Topic mapping</h2>'
            '<table><tr><th>ROS topic</th><th>Message type</th><th>oneM2M path</th></tr>'
            + ''.join(rows) + '</table></html>')


def export_graph(rc, output: Path) -> None:
    import rclpy

    from ipe.adapter.ros2 import GenericROS2Adapter
    from ipe.runtime.discovery import await_graph_convergence
    from ipe.runtime.naming import sanitize_segment
    from ipe.runtime.planning import resolve

    node = ros_node(rc, "ipe_demo_graph")
    try:
        adapter = GenericROS2Adapter(node, lambda _ir: None, lambda *_: None)
        snapshot = await_graph_convergence(
            adapter.snapshot, timeout_sec=rc.discovery["graph_settle_timeout_sec"],
            spin_once=lambda timeout: rclpy.spin_once(node, timeout_sec=timeout),
        ).snapshot
        resolved = resolve(rc.raw, discovered=snapshot)
        paths = {}
        for spec in resolved.topics:
            robot = sanitize_segment(spec.robot_id, rc.naming.get("sanitize", "_"))
            root = f'/{rc.cse.cse_base}/{rc.cse.ae_name}/robots/{robot}/topics'
            paths[spec.interface] = [
                f'{root}/{direction}/{spec.rel_path}' for direction in ("observe", "command")
                if (spec.direction in (direction, "both") and
                    (direction == "observe" or spec.access_enabled))
            ]
        topics = []
        for name, types in sorted(snapshot["topics"]):
            if name in ("/rosout", "/parameter_events"):
                continue
            entry = {"name": name, "types": types, "paths": paths.get(name, [])}
            for key, getter in (("publishers", node.get_publishers_info_by_topic),
                                ("subscribers", node.get_subscriptions_info_by_topic)):
                entry[key] = sorted({f'{i.node_namespace.rstrip("/")}/{i.node_name}'
                                     for i in getter(name)})
            topics.append(entry)
        output.mkdir(parents=True, exist_ok=True)
        (output / "ros-graph.html").write_text(graph_html(topics), encoding="utf-8")
        (output / "ros-graph.json").write_text(json.dumps(topics, indent=2), encoding="utf-8")
        print(f'Graph saved: {output / "ros-graph.html"} ({len(topics)} topics)', flush=True)
    finally:
        node.destroy_node()
        rclpy.shutdown()


def ros_drive(rc, args) -> None:
    import rclpy
    from geometry_msgs.msg import Twist

    node = ros_node(rc, "ipe_demo_driver")
    publisher = node.create_publisher(Twist, "/cmd_vel", 10)
    try:
        deadline = time.monotonic() + 10
        while publisher.get_subscription_count() == 0:
            if time.monotonic() >= deadline:
                raise RuntimeError("No /cmd_vel subscriber discovered.")
            rclpy.spin_once(node, timeout_sec=0.1)

        def send(payload):
            message = Twist()
            message.linear.x = payload["linear"]["x"]
            message.angular.z = payload["angular"]["z"]
            publisher.publish(message)
            rclpy.spin_once(node, timeout_sec=0)
            print(f'ROS /cmd_vel: x={message.linear.x:.2f}, z={message.angular.z:.2f}', flush=True)

        run_motion(send, args.linear, args.angular, args.seconds)
    finally:
        node.destroy_node()
        rclpy.shutdown()


class CSE:
    def __init__(self, rc, origin):
        import requests

        if rc.cse.protocol != "http":
            raise ValueError("This demo client uses HTTP; select cse.protocol=http.")
        self.rc = rc
        self.session = requests.Session()
        self.session.headers.update({"X-M2M-Origin": origin, "X-M2M-RVI": rc.cse.rvi,
                                     "Accept": "application/json"})
        self.root = f'/{rc.cse.cse_base}/{rc.cse.ae_name}'

    def request(self, method, path, **kwargs):
        headers = {"X-M2M-RI": "demo-" + uuid.uuid4().hex}
        if method == "POST":
            headers["Content-Type"] = "application/json;ty=4"
        response = self.session.request(method, self.rc.cse.endpoint.rstrip('/') + path,
                                        headers=headers, timeout=2, **kwargs)
        response.raise_for_status()
        rsc = int(response.headers.get("X-M2M-RSC", "0"))
        if not 2000 <= rsc < 3000:
            raise RuntimeError(f'oneM2M error: RSC={rsc}, {response.text[:200]}')
        return response.json()

    def send(self, payload):
        command_id = 'demo-' + uuid.uuid4().hex
        body = {"m2m:cin": {"cnf": "application/json", "con": json.dumps(
            {"commandId": command_id, **payload}, separators=(",", ":"))}}
        result = self.request("POST", self.root + '/robots/robot/topics/command/cmd_vel', json=body)
        print(f'CSE CIN created: {result.get("m2m:cin", {}).get("ri", "")} '
              f'commandId={command_id} x={payload["linear"]["x"]:.2f} '
              f'z={payload["angular"]["z"]:.2f}; check commandStatus for ROS delivery', flush=True)

    def watch(self):
        paths = ['robots/robot/topics/observe/odom', 'robots/robot/topics/observe/cmd_vel',
                 'status/commandStatus']
        last = {}
        while True:
            for path in paths:
                try:
                    cin = self.request("GET", f'{self.root}/{path}/la')["m2m:cin"]
                    if last.get(path) == cin.get("ri"):
                        continue
                    last[path] = cin.get("ri")
                    con = cin["con"]
                    content = json.loads(con) if isinstance(con, str) else con
                    print(json.dumps({"path": path, "ri": cin.get("ri"), "ct": cin.get("ct"),
                                      "con": content}, ensure_ascii=False), flush=True)
                except Exception as exc:
                    print(f'{path}: {exc}', flush=True)
            time.sleep(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    graph = sub.add_parser("graph", help="Save the live ROS graph as an offline HTML file")
    graph.add_argument("--output", type=Path, default=Path('/artifacts'))
    for name in ("ros-drive", "cse-drive", "stop", "watch"):
        child = sub.add_parser(name)
        if name in ("ros-drive", "cse-drive"):
            child.add_argument("--linear", type=float, default=0.0)
            child.add_argument("--angular", type=float, default=0.0)
            child.add_argument("--seconds", type=float, default=2.0)
        if name != "ros-drive":
            child.add_argument("--origin", default="CAdmin")
    args = parser.parse_args()

    def interrupted(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupted)
    try:
        rc = runtime()
        if args.command == "graph":
            export_graph(rc, args.output)
        elif args.command == "ros-drive":
            ros_drive(rc, args)
        else:
            client = CSE(rc, args.origin)
            try:
                if args.command == "watch":
                    client.watch()
                elif args.command == "stop":
                    run_motion(client.send, 0, 0, 0)
                else:
                    run_motion(client.send, args.linear, args.angular, args.seconds)
            finally:
                client.session.close()
    except KeyboardInterrupt:
        print('Interrupted.', flush=True)
        return 130
    except Exception as exc:
        print(f'Demo failed: {exc}', flush=True)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
