# oneM2M-based Interworking Proxy Entity (IPE)

A generic Interworking Proxy Entity that discovers a live ROS 2 graph and bridges its topics, services, and actions to a oneM2M CSE such as tinyIoT. Interface names, types, directions, robot namespaces, and endpoint QoS come from DDS discovery; deployment profiles and YAML files are not used.

- **Discovery-driven**: the running ROS 2 graph is the source of truth.
- **Type-agnostic**: arbitrary ROS messages are converted to canonical JSON through rosidl reflection.
- **Continuously reconciled**: interfaces are added and removed as the graph changes.
- **Recoverable**: state and pending work survive process and CSE outages.

## Overview

The IPE maps discovered interfaces under one shared AE:

| Subtree | Direction | Purpose |
|---|---|---|
| `robots/<robot>/topics/observe/<iface>/` | ROS 2 → oneM2M | Observed topic data |
| `robots/<robot>/topics/command/<iface>/` | oneM2M → ROS 2 | Command topics |
| `robots/<robot>/services/<iface>/` | Bidirectional | Service request and response |
| `robots/<robot>/actions/<iface>/` | Bidirectional | Action goal, feedback, and result |

Every logical interface has a child `qos` management flexContainer. Topic observe and command resources use `ros:tqos` with different `dir` values, services use `ros:sqos`, and actions use `ros:aqos`. The management resource records Category A resource mappings, Category B behavior status, Category C DDS metadata, and references to the data resources where the policy is applied.

![IPE architecture](assets/architecture.svg)

ROS infrastructure topics (`/rosout`, `/parameter_events`), parameter services, and hidden action internals are excluded automatically. Namespaced endpoint ownership creates robot boundaries. For a graph without robot namespaces, `--robot-id` supplies the fallback oneM2M robot name.

## Requirements

- Python 3.10 or later on Linux
- A ROS 2 environment that provides `rclpy`
- A running robot in the selected DDS domain
- A running tinyIoT CSE

The base Python dependencies are `requests` and `cerberus`. Discovery, plan inspection, and normal execution all require an active ROS 2 graph.

## Installation

```bash
source /opt/ros/humble/setup.bash
python3 -m pip install --upgrade "pip>=24"
python3 -m pip install .
```

The pip 22 bundled with the ROS 2 Humble base image does not read this project's PEP 621
metadata correctly and builds an empty `UNKNOWN-0.0.0` wheel. Upgrade pip before installing.

For MQTT transport:

```bash
python3 -m pip install ".[mqtt]"
```

Installing the package registers the `ipe` command.

## Quick start

Run directly in the current ROS environment:

```bash
ipe --discover
ipe --explain
ipe
```

From the repository, `main.py` also runs in the current ROS environment:

```bash
python3 main.py --discover
python3 main.py --explain
python3 main.py
```

Docker is optional. Use `--docker` only when the host does not provide ROS 2 Humble:

```bash
python3 main.py --docker --discover
python3 main.py --docker
```

The root `Dockerfile` is built automatically only when `--docker` is selected.

The config-free DDS QoS rules, directional behavior, and TurtleBot3 measurements are documented in [ROS2_DDS_QoS_동작_및_검증.md](ROS2_DDS_QoS_동작_및_검증.md).

## Dynamic discovery

Startup waits for a non-empty, stable graph before provisioning resources. The IPE then refreshes the graph periodically and reconciles interface additions, removals, type changes, ownership, and endpoint QoS.

Topic direction is inferred from remote endpoints:

- A remote publisher is an observed topic.
- A remote subscription is a command topic.
- Both endpoint kinds produce a bidirectional topic.

For a bidirectional topic, observation starts immediately but its command publisher stays locked. The first fresh, schema-valid oneM2M command opens an `Allow / Deny` desktop prompt and is always discarded. Approval enables later commands and is retained across restarts for the same robot, topic, and message type. Denial or closing the prompt leaves the command path locked. `main.py --docker` forwards the current X11 display for this prompt; a headless runtime records the request under `config/pendingMappingProposal` and remains locked.

Observed and control interfaces are bridged automatically. To disable discovered command topics, services, and actions, start the IPE in observe-only mode:

```bash
ipe --observe-only
python3 main.py --observe-only
```

Use the default control mode only with a trusted oneM2M deployment. Observe-only mode applies to every discovered control interface in the DDS domain.

## Runtime settings

Deployment values are CLI options or environment variables rather than files:

| CLI option | Environment variable | Default |
|---|---|---|
| `--cse-endpoint` | `IPE_CSE_ENDPOINT` | `http://127.0.0.1:3000` |
| `--cse-base` | `IPE_CSE_BASE` | `TinyIoT` |
| `--cse-timezone` | `IPE_CSE_TIMEZONE` | `local` |
| `--ae-name` | `IPE_AE_NAME` | `ros2-ipe` |
| `--instance-id` | `IPE_INSTANCE_ID` | `ros2-ipe` |
| `--robot-id` | `IPE_ROBOT_ID` | `robot` |
| `--robot-namespace` | `IPE_ROBOT_NAMESPACE` | empty |
| `--domain-id` | `ROS_DOMAIN_ID` | `0` (`main.py --docker`: `30`) |
| none | `RMW_IMPLEMENTATION` | auto-detected from target endpoint GIDs |
| `--ros-peer` | `IPE_ROS_PEER` | none |
| `--refresh-sec` | `IPE_REFRESH_SEC` | `5` |
| `--observe-only` | `IPE_OBSERVE_ONLY` | disabled |

The AE origin defaults to a unique value derived from the AE name and can be overridden with `IPE_CSE_ORIGIN`. Runtime state is stored in `ipe_state.db` or the path specified by `IPE_STATE_DB`.

Finite DDS LIFESPAN values become oneM2M `expirationTime` values. Set `--cse-timezone` to the timestamp timezone used by the CSE; for a KST tinyIoT process, use `--cse-timezone Asia/Seoul`.

When `RMW_IMPLEMENTATION` is unset, an isolated preflight process inspects target endpoint GIDs before `rclpy.init()`. Vendor `01.0f` selects `rmw_fastrtps_cpp` and `01.10` selects `rmw_cyclonedds_cpp`. Mixed, unknown, and empty target graphs stop startup and require an explicit override. An explicitly set `RMW_IMPLEMENTATION` is always preserved.

For Cyclone DDS unicast discovery:

```bash
ipe --domain-id 30 --ros-peer 192.168.219.106
```

`--ros-peer` is used by the Cyclone probe and is applied to the IPE only when Cyclone DDS is selected. It is rejected for non-IP values and is not silently applied to another RMW implementation.

### MQTT CSE transport

Set the transport through environment variables and install the MQTT extra:

```bash
export IPE_CSE_PROTOCOL=mqtt
export IPE_CSE_ID=tinyiot
export IPE_MQTT_HOST=127.0.0.1
export IPE_MQTT_PORT=1883
ipe
```

Optional MQTT variables include `IPE_MQTT_CLIENT_ID`, `IPE_MQTT_QOS`, `IPE_MQTT_USERNAME`, `IPE_MQTT_PASSWORD`, `IPE_MQTT_TLS`, and the `IPE_MQTT_TLS_*` certificate settings. `main.py --docker` forwards these variables into its container.

The broker and the CSE MQTT binding must both be active before the IPE starts. For tinyIoT,
build the CSE with `ENABLE_MQTT` and confirm that it subscribed to the oneM2M request and
response topics. Installing Mosquitto alone does not enable MQTT in a CSE binary that was
built without that option.

## CLI

```text
ipe [options]
```

| Flag | Action |
|---|---|
| `--discover` | Print one converged ROS 2 graph snapshot |
| `--explain`, `--dry-run` | Discover the graph and print the resolved binding plan |
| `--bootstrap-only` | Discover and provision CSE resources, then exit |
| `--observe-only` | Disable discovered command topics, services, and actions |
| `--reset` | Delete the existing AE before normal startup |
| `--log-level` | Set `DEBUG`, `INFO`, `WARNING`, or `ERROR` logging |
