# LCA Stack

A lightweight **messaging + logging stack** for collaborative autonomy experiments across simulation and real hardware.

LCA Stack standardizes how agents communicate and how experiments are recorded. Each agent runs a small **Agent Daemon** that handles networking and logging. Autonomy code can be written in any language and connects locally to the daemon. Platform-specific details are isolated behind an **Adapter**.

---

## Table of contents
- [LCA Stack](#lca-stack)
  - [Table of contents](#table-of-contents)
  - [Overview](#overview)
  - [Quickstart](#quickstart)
    - [Requirements](#requirements)
    - [Install](#install)
    - [Run the Agent Daemon](#run-the-agent-daemon)
    - [Connect an Adapter and Autonomy](#connect-an-adapter-and-autonomy)
    - [Run artifacts](#run-artifacts)
    - [Example adapters and autonomy](#example-adapters-and-autonomy)
  - [Key concepts, terminology, and acronyms](#key-concepts-terminology-and-acronyms)
    - [Core terms](#core-terms)
    - [Acronyms](#acronyms)
  - [Messaging model](#messaging-model)
    - [Topics](#topics)
    - [Message types](#message-types)
    - [Common header](#common-header)
    - [Delivery settings (QoS)](#delivery-settings-qos)
  - [Logging and run artifacts](#logging-and-run-artifacts)
    - [MCAP logs (per agent)](#mcap-logs-per-agent)
    - [Run manifest](#run-manifest)
    - [Typical directory layout](#typical-directory-layout)
  - [Architecture](#architecture)
    - [System layout](#system-layout)
    - [Responsibilities](#responsibilities)
      - [Platform Adapter](#platform-adapter)
      - [Autonomy Process](#autonomy-process)
      - [Agent Daemon](#agent-daemon)
    - [Interfaces](#interfaces)
      - [1) Team Bus Interface (DDS)](#1-team-bus-interface-dds)
      - [2) Local Link Interface (IPC)](#2-local-link-interface-ipc)
    - [End-to-end flow](#end-to-end-flow)
      - [A) Local control loop (inside one agent)](#a-local-control-loop-inside-one-agent)
      - [B) Team communication loop (between agents)](#b-team-communication-loop-between-agents)
      - [C) Status and run events](#c-status-and-run-events)
  - [Safety model](#safety-model)
  - [Portability across simulation and real hardware](#portability-across-simulation-and-real-hardware)

---

## Overview

LCA Stack is a distributed system pattern and reference implementation for multi-agent experiments:

- **DDS (Data Distribution Service)** is used for real-time publish/subscribe messaging between agents.
- **MCAP (Message Capture)** is used for recording time-stamped message streams to portable log files.

The stack is designed so that:
- Communication and recording are consistent across agents.
- Autonomy logic is language-agnostic.
- Platform integrations (simulators and robots) are swapped by changing only the adapter layer.

---

## Quickstart

### Requirements
- Python 3.10+
- A platform-specific **Adapter** process and an **Autonomy** process (any language). Both connect locally to the Agent Daemon.

### Install

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

This installs dependencies and installs `lca-stack` in editable mode from the repo (so `python -m lca_stack...` and `lca-daemon` work from your checkout).

### Run the Agent Daemon

Start the daemon first. It waits for exactly two local TCP connections: one from the Adapter and one from Autonomy.

```bash
python3 -m lca_stack.daemon.cli --agent-id cf1 --adapter-port 5001 --autonomy-port 5002
```

If installed, you can also use the console script:

```bash
lca-daemon --agent-id cf1 --adapter-port 5001 --autonomy-port 5002
```

### Connect an Adapter and Autonomy

- **Adapter** connects to the daemon’s **adapter port** (example: `5001`) and publishes platform observations as JSONL.
- **Autonomy** connects to the daemon’s **autonomy port** (example: `5002`) and publishes actuation requests as JSONL.

On connect, the daemon immediately sends a one-line **handshake** (`kind: lca_handshake_v1`) so both sides learn:
- `run_id` (daemon-generated; source of truth for artifacts)
- `agent_id` (daemon config)
- `protocol_version`
- ports + scenario/seed (if provided on the daemon CLI)

After the handshake, processes exchange newline-delimited JSON objects. Each message should include the common `header` fields described below. In Python you can use `lca_stack.ipc.make_header(...)`.

If a message arrives without a valid header, the daemon will **inject** one using daemon time, mark the payload with `lca_meta.header_injected: true`, and emit a `run/event` warning.

### Run artifacts

Each daemon invocation corresponds to a single run. The daemon generates a fresh `run_id` at startup and creates a run directory immediately:

```
runs/<run_id>/
  manifest.yaml
  logs/
    <agent_id>.mcap
```

The daemon writes `manifest.yaml` at run start and finalizes it at run stop. Run lifecycle and diagnostic messages are published on the `run/event` topic (including `run_start`, `run_stop`, disconnects, and warnings).

### Example adapters and autonomy

This repo includes example implementations under `examples/`. They are optional and meant as reference adapters/autonomy processes for the daemon IPC.

Example (Spot velocity teleop):

```bash
python3 examples/autonomy/spot_keyboard.py --host 127.0.0.1 --port 5002 --agent-id cf1
```

For simulator-specific wiring (for example Webots `controllerArgs`), see the example adapter directory under `examples/`.

---

## Key concepts, terminology, and acronyms

### Core terms
- **Agent**: A participant in the experiment (robot, simulated actor, or human interface).
- **Process**: A running program on an agent (often called a “node” in robotics).
- **Platform Adapter (Adapter)**: A platform-facing process that talks to a simulator or hardware API and applies actuation.
- **Autonomy Process**: Decision-making code (planning, control, coordination). Any language/framework.
- **Agent Daemon**: Standard infrastructure process that provides:
  - DDS messaging (publish/subscribe)
  - MCAP recording (flight recorder)
  - Safety Guard (local command checks)
  - Run lifecycle events and run manifest

### Acronyms
- **API** (Application Programming Interface): A defined way for software components to communicate.
- **DDS** (Data Distribution Service): An industry-standard publish/subscribe messaging system for distributed real-time systems.
- **QoS** (Quality of Service): Delivery settings for messages (for example: reliable vs best-effort).
- **MCAP** (Message Capture): A file format for recording time-stamped message streams.
- **IPC** (Inter-Process Communication): Communication between processes on the same machine.
- **TCP** (Transmission Control Protocol): A reliable network protocol commonly used for local sockets.
- **UDP** (User Datagram Protocol): A lightweight network protocol commonly used for low-latency messaging.
- **UUID** (Universally Unique Identifier): A unique ID used to label a run (`run_id`) or other artifacts.

---

## Messaging model

DDS communication is organized into **topics** (named streams). A topic carries one message type.

### Topics

Standard topics:
- `team/command` — Commands intended for other agents or groups.
- `team/message` — Coordination messages (plans, intents, role announcements).
- `agent/<agent_id>/status` — Heartbeat and health status.
- `run/event` — Run lifecycle and safety events.

### Message types

- **Command**: Structured instruction directed to a target agent or group.
- **Status**: Heartbeat and health (mode, faults, emergency stop state, etc.).
- **TeamMessage**: Coordination information that is not a direct command.
- **Event**: Lifecycle and safety events (run start/stop, safety clamp, fault transitions).

### Common header

Every message recorded or transmitted should include a common header to support analysis and debugging:

- `run_id` (UUID): identifies the run
- `agent_id` (string): identifies the sender
- `seq` (integer): monotonically increasing per sender and topic
- `t_mono_ns` (integer): monotonic timestamp in nanoseconds (time since boot)
- `t_wall_ns` (integer): wall-clock timestamp in nanoseconds (system time)

Why these fields exist:
- `seq` detects drops/reordering per sender.
- `t_mono_ns` provides strict ordering within one process / machine.
- `t_wall_ns` enables cross-agent alignment **only** when you have a clock sync / offset model.

Time model:
- `t_mono_ns`: **originating process monotonic time**. Use this for ordering on that machine. It should be non-decreasing per `(origin_agent_id, origin_topic)`.
- `t_wall_ns`: **originating process wall time**. Cross-agent comparisons require time synchronization (e.g., **PTP** via linuxptp, or NTP as a looser alternative) or an explicit offset model.
- MCAP `log_time`: **daemon receive time** (wall-like time computed from a monotonic anchor so it won’t go backward). This is reliable for local ordering inside one daemon even if the system wall clock jumps.

Message identity / alignment:
- Every payload should include a `topic` string (do not rely only on the MCAP channel name).
- Every payload should include an origin key so the “same message” is matchable across agents/logs:
  - `origin_agent_id`, `origin_seq`, `origin_topic`

The daemon will inject/normalize `topic` and origin fields if they are missing.

### Delivery settings (QoS)

DDS supports **QoS (Quality of Service)** settings per topic. Typical choices:
- Commands: **reliable**
- Status: **best effort**
- Coordination messages: reliable or best effort depending on semantics

QoS must be consistent across publishers and subscribers on the same topic.

---

## Logging and run artifacts

Each run produces:
- **MCAP logs** (one per agent), recording:
  - outgoing DDS messages (published)
  - incoming DDS messages (received)
  - local adapter observations and actuation
  - run lifecycle and safety events
- **Run manifest** (one per run), describing:
  - run ID and timing
  - participating agents
  - scenario name/version
  - software version identifiers
  - configuration parameters and random seed
  - outcomes/notes (optional)

### MCAP logs (per agent)

Each agent writes an MCAP file containing time-stamped streams for:
- `local/adapter/observation` — observations sent from Adapter → Daemon (and forwarded to Autonomy)
- `local/autonomy/actuation_request` — requests sent from Autonomy → Daemon
- `local/adapter/actuation` — the final actuation the Adapter receives (post-safety; currently pass-through)
- `run/event` — lifecycle + diagnostics (handshake, `run_start`, `run_stop`, disconnects, warnings)

MCAP timestamps:
- `publish_time` is taken from the message header (`t_wall_ns`, i.e., the originating process wall time).
- `log_time` is the daemon receive time (computed from a monotonic anchor).

Recording both **sent** and **received** DDS messages matters: it preserves what each agent actually observed on the network.

### Run manifest

Each run produces a small manifest file that describes:
- `run_id` + `agent_id`
- `host` + ports (`adapter`, `autonomy`)
- `start_wall` / `end_wall`
- scenario + seed (if provided)
- clock anchors + a lightweight clock health snapshot (offset/spread diagnostics)

### Typical directory layout

```
runs/<run_id>/
  manifest.yaml
  logs/
    <agent_id>.mcap
    <agent_id>.mcap
    ...
```

---

## Architecture

This project provides a **distributed messaging and logging backbone** for multi-agent experiments. Each agent runs a small, standard **Agent Daemon** responsible for networking and recording. Autonomy code (the “brains”) can be written in any language and connects locally to the daemon. Platform-specific control (simulator or hardware) is isolated behind an **Adapter**.

### System layout

Each agent runs three roles. Only the Adapter depends on the specific simulator or robot.

```
+--------------------------------------------------------------+
|                           Agent                              |
|                                                              |
|  +----------------+      IPC (local)      +----------------+ |
|  |  Platform      | <-------------------> |  Agent Daemon   | |
|  |  Adapter       |                       |                 | |
|  | (sim/hardware) |                       | - DDS messaging | |
|  +----------------+                       | - MCAP logging  | |
|                                           | - Safety guard  | |
|  +----------------+      IPC (local)      | - Run events    | |
|  |  Autonomy      | <-------------------> +----------------+ |
|  |  Process       |                                         |
|  | (any language) |                                         |
|  +----------------+                                         |
+--------------------------------------------------------------+

              DDS network (publish/subscribe)
         <---------------------------------------->
                   Other agents’ daemons
```

### Responsibilities

#### Platform Adapter
The Adapter is the only platform-specific component.

- Reads platform state (sensors, odometry, mode, faults).
- Applies platform actuation (motor commands, velocity setpoints, etc.).
- Communicates **locally** with the Agent Daemon using a simple IPC protocol (often TCP on localhost).

The Adapter does **not** speak DDS.

#### Autonomy Process
The Autonomy Process contains the experiment logic.

- Consumes observations and team messages.
- Produces high-level decisions (role changes, goals, commands) and local actuation requests.
- Can be written in any language and can use any internal architecture (state machine, behavior tree, MPC, RL policy, etc.).
- Communicates **locally** with the Agent Daemon via the daemon’s local API.

The Autonomy Process does **not** need to implement DDS or MCAP.

#### Agent Daemon
The Agent Daemon provides the standard experiment infrastructure.

- **DDS Messaging**
  - Publishes outgoing team messages and status.
  - Subscribes to incoming team messages and commands.
- **MCAP Recording**
  - Records inbound/outbound DDS traffic.
  - Records local observations and actuation exchanged with Adapter and Autonomy.
  - Records run lifecycle and safety events.
- **Safety Guard**
  - Validates outgoing actuation against local constraints.
  - Can clamp, override, or stop actuation.
  - Emits events explaining interventions.
- **Run Lifecycle**
  - Starts/stops recording.
  - Emits run events (`run_start`, `run_stop`, etc.).
  - Produces a run manifest.

### Interfaces

The architecture uses two different interfaces.

#### 1) Team Bus Interface (DDS)
DDS is used for communication **between agents**. Communication is organized into **topics**, such as `team/command`.

DDS supports **QoS (Quality of Service)** settings per topic. Common choices:
- Commands: **reliable** (avoid missing critical stop/role-change commands).
- Status: **best effort** (next heartbeat arrives soon).
- Coordination messages: reliable or best effort depending on semantics.

#### 2) Local Link Interface (IPC)
IPC is used for communication **within an agent**, between processes on the same machine.

There are typically two local links:
- Adapter ⇄ Agent Daemon
- Autonomy Process ⇄ Agent Daemon

A simple approach is newline-delimited JSON over localhost TCP during early development, later replaced by a stricter schema (for example, Protocol Buffers) once the message fields stabilize.

### End-to-end flow

#### A) Local control loop (inside one agent)
This loop drives the platform.

1. **Adapter** reads platform state and sends an **Observation** to the Agent Daemon (IPC).
2. **Agent Daemon** forwards the Observation to the Autonomy Process (IPC) and records it to MCAP.
3. **Autonomy Process** updates its controller and sends an **Actuation Request** to the Agent Daemon (IPC).
4. **Agent Daemon** runs the Safety Guard:
   - If safe, pass through.
   - If unsafe, clamp/override/stop and emit a **Safety Event**.
5. **Agent Daemon** sends the final **Actuation** to the Adapter (IPC) and records it to MCAP.
6. **Adapter** applies the actuation to the simulator or hardware.

#### B) Team communication loop (between agents)
This loop enables coordination.

1. **Autonomy Process** decides to communicate (for example: publish a rally point, request a role change, broadcast stop).
2. **Autonomy Process** sends a **TeamMessage** or **Command** to the Agent Daemon (IPC).
3. **Agent Daemon** publishes that message to the appropriate DDS topic and records the outgoing message to MCAP.
4. Other agents’ daemons receive the message via DDS, record it to MCAP, and forward it locally to their Autonomy Processes (IPC).
5. Each Autonomy Process reacts using its own local controller, producing local actuation and/or further team messages.

#### C) Status and run events
In parallel with (A) and (B):

- Each Agent Daemon periodically publishes `agent/<agent_id>/status` (DDS) and records it.
- The Agent Daemon emits `run/event` events:
  - `run_start` when recording begins
  - `run_stop` when the run terminates
  - safety interventions
  - fault transitions

---

## Safety model

The Safety Guard runs inside the Agent Daemon and sits on the command path to the platform.

Typical checks:
- Mode gating (ignore actuation unless the run is in “running” mode)
- Bounds checks (speed, acceleration, workspace limits)
- Fault response (transition to safe-stop on fault)
- Emergency stop handling

Every intervention generates an Event that is recorded and can be displayed during post-run analysis.

---

## Portability across simulation and real hardware

Portability comes from keeping platform APIs out of the team bus:

- Simulation vs real hardware changes only the **Adapter**.
- The **Agent Daemon** and **Autonomy Process** keep the same interfaces.
- DDS topics and message schemas remain stable across environments.
- MCAP logs and analysis tools remain unchanged.

This allows running the same autonomy code in a simulator today and on physical robots later by swapping the adapter implementation.
