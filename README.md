# Explainable Agentic Root Cause Analysis for Congestion Management in 5G Networks: Detecting Service Deterioration and Autonomous Mitigation Decisions

An agentic pipeline that attributes 5G core congestion to its responsible network function and recommends a mitigation, with an independent verification stage checking the attribution before it is used.

*Module CSI_7_PRO, Department of Computer Science and Informatics, London South Bank University*<br>

Supervised by Professor Anastasios Dagiuklas <br>
*Student: [Heven Tafese](https://github.com/HevenTafese)*<br>
## Repository structure

* `congestion_fusion.py`
* `agents/`
  * `alert/alert_agent.py`
  * `configuration/configuration_agent.py`
  * `performance/performance_agent.py`
* `data/`
  * `captures/`
  * `chroma_db/`
  * `ietf_docs/`
  * `kb/`
  * `deployed_instances.json`
  * `kb.sqlite`
  * `mitigation_full_history.jsonl`
  * `mitigation_history.jsonl`
* `explanation/`
  * `data/`
  * `explanation_agent.py`
* `generator/`
  * `gnb_downlink/`
  * `e2e_cp_npdu.sh`
  * `n2_ramp.sh`
  * `n6_iperf_dyn.sh`
  * `run_n3_vm2.sh`
* `knowledge_base/`
  * `ingest/`
  * `scheduler/`
* `mcp_board/mcp_board.py`
* `mitigation/mit.py`
* `mnf/`
  * `gnb_downlink/`
  * `e2e_mnf.py`
  * `gnb_mnf.py`
  * `n2_mnf.py`
  * `n4_mnf.py`
  * `n6_mnf.py`
  * `run_n3_vm1.sh`
  * `tshark_mnf.py`
* `rca/`
  * `data/`
  * `agentic_rca.py`
  * `run_rca_a2a.py`
  * `rca_pipeline/agents/shared/`
    * `rca_core.py`
    * `schema_discovery.py`
    * `thresholds.py`
* `runner/`
* `shared/`
* `verification/`
* `migrate_baselines.py`
* `README.md`
* `requirements.txt`


*Submitted August 2026*

## Table of contents

* [Overview](#overview)
* [Why this matters](#why-this-matters)
* [System architecture](#system-architecture)
* [Testbed and scenarios](#testbed-and-scenarios)
* [Requirements](#requirements)
* [Installation](#installation)
* [Usage](#usage)
* [Results](#results)
* [Repository structure](#repository-structure)
* [Limitations and future work](#limitations-and-future-work)
* [Acknowledgments](#acknowledgments)
* [License](#license)

## Overview

This project designs, builds and evaluates an agentic pipeline that attributes congestion in a 5G core network to the responsible network function and selects a mitigation appropriate to the plane at fault. Control plane signalling overload and user plane data saturation often produce the same symptoms while calling for opposite corrective actions. Detection and classification alone report that congestion exists without establishing why, so the attributed cause is checked against observable telemetry through an independent verification stage before a mitigation is chosen. Explainability functions here as the safeguard that makes an autonomous attribution trustworthy enough to act on.

## Why this matters

5G core networks separate control plane and user plane functions, and control plane signalling has grown faster than user plane traffic in recent years. Both planes can degrade service under load, so an operator needs more than a congestion alert. This project treats attribution, evidence correlation and mitigation as three connected requirements rather than three separate tools, because service assurance in a live network depends on knowing which function to act on, not only that something is wrong.

## System architecture

The system is a layered, closed loop pipeline rather than a one way detection tool. Each layer reads from the one below it, and the mitigation layer's output feeds back into the network being monitored.

Figure coming.

**Measurement layer**

A dedicated management function for each interface reads live traffic and computes a utilisation figure, rho, against a calibrated ceiling once per second. Utilisation below 0.85 is normal, from 0.85 up to 1.0 is onset, and 1.0 or above is congestion. A function that stays in the congestion band for 75 continuous seconds latches to FAILING and remains there.

**Detection layer**

Three agents, Performance, Alert and Configuration, read these records with no language model involved. Performance compares utilisation against the calibrated ceiling. Alert fits a curve to recent samples to catch a rate of change before utilisation itself crosses the line. Configuration checks the running configuration against a golden baseline and a set of legal procedure orders. Each agent publishes its own observation to a shared coordination board.

**Reasoning layer**

The RCA agent reads the published observations, retrieves supporting evidence from the knowledge base, and forms its own attribution. It does not adopt a detection agent's published verdict as its own conclusion, and it rederives each interface's congestion band directly from the utilisation model rather than trusting a detection agent's label.

**Explanation layer**

A separate agent checks the committed attribution against the recorded evidence, tests which evidence was necessary to the decision by removing it and reapplying the rule, and produces a verified account of the reasoning rather than accepting the model's own explanation of itself.

**Mitigation layer**

A separate deterministic agent computes the corrective action using a model predictive approach, projecting each candidate action's utilisation over a short horizon before scoring it. This stage uses no language model for the decision itself, the language model is only asked to justify an action after it has already been selected. Earlier designs using a fixed decision table, a DMN style policy and fuzzy logic control were evaluated and set aside before this approach was adopted. Actions are computed and recorded but not applied to the running core in this testbed.

A message bus called MCP sits between the detection agents and the reasoning layer. It stores and forwards observations by address and does not interpret their content, so deciding whether something counts as congestion is entirely the reasoning layer's job.

**Core components**

Measurement functions, one per interface
* mnf/n2_mnf.py
* mnf/n4_mnf.py
* mnf/n6_mnf.py
* mnf/e2e_mnf.py
* mnf/gnb_mnf.py
* mnf/tshark_mnf.py, a shared tshark based capture helper

Detection agents, LangGraph, no language model
* agents/alert/alert_agent.py
* agents/configuration/configuration_agent.py
* agents/performance/performance_agent.py

Coordination
* mcp_board/mcp_board.py, the MCP message bus, FastMCP over streamable HTTP on port 9000

Reasoning, explanation and mitigation, LangGraph
* rca/agentic_rca.py, the RCA agent, six nodes
* rca/run_rca_a2a.py, the runner that drains the MCP board and drives the RCA agent
* explanation/explanation_agent.py, ten nodes
* mitigation/mit.py, the mitigation agent, eight nodes
* rca/rca_pipeline/agents/shared/rca_core.py, the shared formula module

**Knowledge base layer**

* ChromaDB, four collections. Normative holds 4,529 chunks from five 3GPP specifications. Remedial holds 320 chunks from seven academic papers and one ETSI report. External graphs holds 169 chunks from O RAN architecture documents and papers on explainable AI and AI agents in telecoms. Empirical holds distilled records from each testbed capture.
* Neo4j, the causal graph, 100 nodes and 107 edges, every edge linked to a 3GPP clause
* SQLite, six tables covering interface baselines, verification rules, KPI definitions, mitigation actions, literature baselines and scenario captures

## Testbed and scenarios

The testbed spans three hosts on an isolated host only network.

* Core host, 192.168.56.10, VirtualBox Ubuntu, runs free5GC v3.4.1, the measurement functions, all agents, and the knowledge base
* Generator host, 192.168.56.20, VirtualBox Ubuntu, runs UERANSIM, PacketRusher and the traffic generators
* Model host, 192.168.56.1, Windows and WSL2, runs Ollama on a GPU, serving the reasoning model

Six congestion scenarios provide the evaluation basis, each calibrated to its own interface ceiling.

* N2, control plane, gNB to AMF signalling, ceiling 100 messages per second
* N4, control plane, SMF to UPF session management, ceiling 2,000 messages per second
* N3, user plane, gNB to UPF data, ceiling 18,500 packets per second or 138 Mbps, whichever is higher
* N6, user plane, UPF to data network egress, ceiling 24 Mbps, provisional
* gNB downlink, user plane, forwarding path saturation, ceiling 450 packets per second
* End to end, control plane, a combined ramp across AMF, SMF and UPF, with the SMF as the calibrated saturation point at 110 packets per second on N11

Confirmed findings per scenario, shared evaluation window.

* N2 holds the weakest detection accuracy of the six scenarios, yet root cause naming stays strong
* N4 reaches strong detection accuracy and is the only scenario producing an outright wrong attribution rather than an abstention
* N3 matches every band and names the correct function on every confirmed cycle. The scenario is bound by packet rate rather than bandwidth, since real packets average between 150 and 1000 bytes, below the roughly 930 bytes at which the two limits would saturate together
* N6 stays strong on both axes, with abstention rather than a wrong call as its main gap
* gNB downlink matches a usable band on every confirmed cycle, yet shows the weakest root cause naming of the six. The bottleneck sits at the forwarding and sender level rather than at the gNB's own processor, which stayed under 40% utilisation throughout
* End to end matches every band and names the correct function on every confirmed cycle. The SMF reached a peak utilisation of approximately 3.1 during the confirmed run, the clearest single saturation point recorded across the six scenarios

## Requirements

The reference implementation ran on a single laptop with 32GB of system memory and a 4GB VRAM GPU, split across the three hosts described above.

**Pipeline dependencies**

* Python
* LangGraph, agent orchestration for detection, reasoning, explanation and mitigation
* FastMCP, the coordination message bus
* Ollama, serving `qwen3:4b-instruct-2507-q4_K_M` under the local tag rca-qwen
* Neo4j
* ChromaDB
* SQLite

The reasoning model went through several iterations before reaching this choice. An initial phi4 mini model did not support the structured tool calling the pipeline requires. Gemma 3 4B and Llama 3.2 3B were evaluated next but produced weaker structured reasoning. Qwen3 8B exceeded the available GPU memory. Qwen3 4B provided native tool calling, but its thinking mode took 10 to 12 minutes per cycle, so the non thinking variant became the final choice.

**Testbed and traffic generation**

* free5GC v3.4.1, built from source
* gtp5g, the GTP-U kernel module, built from source
* gtp5g-tunnel (free5gc/libgtp5gnl), used for direct PDR and FAR injection on the N3 scenario
* UERANSIM, built from source
* PacketRusher, built from source
* go-pfcp (wmnsk) v0.0.24, used for the N4 heartbeat generator
* MongoDB, subscriber and policy data

A pinned dependency list belongs in `requirements.txt` at the repository root.

## Installation

1. Provision three hosts on an isolated host only network, one core host, one generator host, one model host with a GPU
2. Clone this repository onto the core host
3. Build free5GC v3.4.1, gtp5g and gtp5g-tunnel from source, following free5GC's own installation guide
4. Build UERANSIM and PacketRusher from source on the generator host
5. Provision the subscriber base, expanded to 1,500 entries for this project
6. Install the Python dependencies from `requirements.txt`
7. Pull and serve `qwen3:4b-instruct-2507-q4_K_M` through Ollama on the model host, tagged rca-qwen
8. Start `mcp_board/mcp_board.py`, then the three detection agents, then `rca/run_rca_a2a.py`

## Usage

Each scenario has its own generator script under `generator/`. The N2 ramp runs `generator/n2_ramp.sh`, the end to end control plane ramp runs `generator/e2e_cp_npdu.sh`, and N6 uses `generator/n6_iperf_dyn.sh`. The corresponding management function under `mnf/` records the interface's utilisation while a scenario runs. The RCA pipeline itself starts with `python rca/run_rca_a2a.py`, which drains the MCP board and feeds the reasoning engine continuously.

## Results

Chapter 5 verification is complete across all six scenarios. Detection and root cause outcome by scenario, shared evaluation window.

| Scenario | Confirmed congestion cycles | Detection accuracy | Root cause correct | Root cause wrong | Root cause null |
| --- | --- | --- | --- | --- | --- |
| N2 | 86 | 91.3% | 98.8% | 0% | 1.2% |
| N3 | 127 | 100% | 100% | 0% | 0% |
| N4 | 124 | 98.3% | 96.0% | 3.2% | 0.8% |
| N6 | 106 | 100% | 96.2% | 0% | 3.8% |
| gNB downlink | 148 | 95.9% | 83.1% | 0% | 16.9% |
| End to end | 74 | 100% | 100% | 0% | 0% |

The N4 figures above were pending one recount as of the last session covering this table, worth a final check before this goes live.

N2 and gNB downlink sit at opposite corners of the same picture. N2 holds the weakest detection accuracy of the six scenarios yet names the correct function in almost every cycle that returns an answer. gNB downlink reverses this, matching a usable band on every confirmed cycle while naming the correct function least often of the six, abstaining rather than naming the wrong one. N4 stands apart as the only scenario where the pipeline named an outright wrong function rather than abstaining, on 3.2% of its cycles, despite strong detection accuracy of its own.

Beyond detection and attribution, the audit layer that independently re derives each verdict passed 99.73% of investigation cycles, agreed with the model on 99.6% of calibrated cycles, and grounded 97.7% of explanations in the underlying evidence. The mitigation agent's action selection reconciled exactly against the full decision log across all six interfaces over the confirmed evaluation window. Median pipeline latency held at 25.1 seconds on user plane interfaces and 28.0 seconds on control plane interfaces, with the interval between cycles measuring 66.3 seconds on user plane interfaces and 67.6 seconds on control plane interfaces.

These figures should match the final submitted chapter, worth one last check since dissertation numbers can move during final edits.

## Repository structure

* `congestion_fusion.py`
* `agents/`
  * `alert/alert_agent.py`
  * `configuration/configuration_agent.py`
  * `performance/performance_agent.py`
* `data/`
  * `captures/`
  * `chroma_db/`
  * `ietf_docs/`
  * `kb/`
  * `deployed_instances.json`
  * `kb.sqlite`
  * `mitigation_full_history.jsonl`
  * `mitigation_history.jsonl`
* `explanation/`
  * `data/`
  * `explanation_agent.py`
* `generator/`
  * `gnb_downlink/`
  * `e2e_cp_npdu.sh`
  * `n2_ramp.sh`
  * `n6_iperf_dyn.sh`
  * `run_n3_vm2.sh`
* `knowledge_base/`
  * `ingest/`
  * `scheduler/`
* `mcp_board/mcp_board.py`
* `mitigation/mit.py`
* `mnf/`
  * `gnb_downlink/`
  * `e2e_mnf.py`
  * `gnb_mnf.py`
  * `n2_mnf.py`
  * `n4_mnf.py`
  * `n6_mnf.py`
  * `run_n3_vm1.sh`
  * `tshark_mnf.py`
* `rca/`
  * `data/`
  * `agentic_rca.py`
  * `run_rca_a2a.py`
  * `rca_pipeline/agents/shared/`
    * `rca_core.py`
    * `schema_discovery.py`
    * `thresholds.py`
* `runner/`
* `shared/`
* `verification/`
* `migrate_baselines.py`
* `README.md`
* `requirements.txt`

## Limitations and future work

Coming soon.

## Acknowledgments

This project would not exist in its current form without the guidance of Professor Anastasios Dagiuklas. His supervision shaped this dissertation from an early, uncertain idea into a working pipeline with a real testbed behind every claim, and his direction gave the whole project its centre of gravity. Thank you for the patience, the sharp questions, and the trust to let this project become what it needed to become.

