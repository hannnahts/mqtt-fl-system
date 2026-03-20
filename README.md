# [cite_start]Communication-Efficient Federated Learning in MQTT-based Edge-Cloud Systems [cite: 417, 418]

[cite_start]This repository contains the source code for a hierarchical multi-broker federated learning (FL) system operating over the Message Queuing Telemetry Transport (MQTT) protocol[cite: 474, 475]. [cite_start]This system is designed to address communication overhead and single-broker congestion issues inherent in traditional centralized MQTT-based FL frameworks for resource-constrained IoT environments[cite: 438, 439, 453, 454].

[cite_start]The prototype demonstrates a practical application using a real-world air quality dataset from Newcastle to collaboratively train a linear regression model without exposing raw edge data to the cloud[cite: 442, 448].

## System Architecture

[cite_start]The core innovation of this project is a decoupled, hierarchical edge-cloud topology that migrates from a baseline single-broker architecture to a tiered multi-broker design[cite: 478].

### Structural Overview

[cite_start]The system is structured across three primary logical layers[cite: 479]:

1.  [cite_start]**Edge Layer:** Emulates multiple distributed clients, each maintaining a local data shard[cite: 480]. [cite_start]Edge devices are isolated from cloud-side network volatility by connecting exclusively to an Edge-Local Broker[cite: 481, 482].
2.  [cite_start]**Broker Layer:** Forms the communication backbone, divided into one Edge Broker and two parallel Cloud Brokers (Cloud-A and Cloud-B)[cite: 483, 487]. [cite_start]The Edge Broker acts as a transparent bridge, distributing upstream client updates across Cloud-A and Cloud-B to prevent single-point congestion[cite: 488].
3.  [cite_start]**Cloud Layer:** Hosts the Orchestrator, which manages training rounds via control messages, and the Aggregator, which collects model updates from both cloud paths and disseminates the global model through Cloud-B[cite: 489, 490].

The detailed logical structure and communication paths are illustrated in the diagram below:

<p align="center">
  <img src="new_architecture.png" width="90%" alt="System Architecture Diagram">
</p>

**Figure 1: Hierarchical MQTT-based system architecture, showing isolated Edge Broker, routed Cloud Brokers A/B, and separate Control, Uplink, and Downlink Planes.**

### Workflow

[cite_start]To facilitate lightweight and efficient communication, the system executes a synchronous FL workflow[cite: 492]:

<p align="center">
  <img src="stepwise_workflow.png" width="90%" alt="System Stepwise Workflow">
</p>

**Figure 2: Stepwise communication and computation workflow showing control message flow, local training, routed update upload, aggregation, and model download.**

* [cite_start]**Initialization:** Clients load local data shards and perform an initial training round using warm-start initialization[cite: 496].
* [cite_start]**Synchronous Rounds:** The cloud orchestrator advances training rounds via control messages[cite: 494].
* [cite_start]**Compression:** To maximize communication efficiency, update payloads and global models are serialized and compressed using Gzip prior to transfer[cite: 498].

## Air Quality Prediction Task

[cite_start]The system collaboratively trains a lightweight linear-regression model to predict fine particulate matter (PM2.5) concentrations[cite: 492, 506]. [cite_start]The continuous input features sourced from the Newcastle Centre dataset include[cite: 505]:

* [cite_start]**PM10:** Concentration of inhalable particulate matter[cite: 508].
* [cite_start]**NO and NO2:** Concentrations of nitric oxide and nitrogen dioxide, representing primary combustion-related environmental pollutants[cite: 509].
* [cite_start]**O3:** Concentration of ozone, providing additional atmospheric and chemical context[cite: 510].

## Repository Structure

The core components of the prototype implementation include:

* [cite_start]`client.py`: Handles the local training workflow using gradient descent and warm-start initialization[cite: 495, 496].
* [cite_start]`aggregator.py`: Implements cloud aggregator logic and the Federated Averaging (FedAvg) algorithm[cite: 500].
* [cite_start]`orchestrator.py`: Manages the cloud-side synchronization, advancing FL rounds[cite: 490].
* [cite_start]`mosquitto.conf`: Configuration files defining the MQTT broker routing policies and bridging rules[cite: 459].
* [cite_start]`runtime_metrics.py`: A continuous metrics module that tracks CPU time, peak memory footprint, and topic-level byte counts[cite: 502].
* [cite_start]`prepare_dataset.py`: Handles raw data preprocessing in compliance with paper specifications[cite: 318].

## Evaluation and Key Results

[cite_start]The system was benchmarked against a centralized single-broker baseline without compression[cite: 526]. [cite_start]Both scenarios were tested under an identical workload: 5 clients completing 3 federated learning rounds[cite: 524, 525].

Experimental results demonstrate:

* [cite_start]**Communication Efficiency:** The proposed hierarchical architecture reduced total transmitted wire bytes by 13.21% (from 3748 bytes to 3253 bytes) while preserving the same logical FL workflow[cite: 550, 663]. [cite_start]The gain was primarily driven by the Gzip compression applied specifically to model update messages[cite: 555].
* [cite_start]**Computation Trade-off:** The proposed configuration traded a modest, quantifiable increase in computational cost (e.g., slight increases in client CPU usage and training latency) for the bandwidth reduction[cite: 605, 646]. [cite_start]This trade-off validates the proposed system as a sensible engineering compromise for IoT networks[cite: 648].

## Contributors

[cite_start]This project was developed by the following researchers at Newcastle University[cite: 419, 421, 423, 427, 429, 432]:

* [cite_start]**Yichun Gan:** Team Leader & Broker Layer implementation[cite: 697].
* [cite_start]**Yue Han:** Deputy Team Leader, System Design & Main Author[cite: 697].
* [cite_start]**Haochen Guo:** Aggregator & Client Implementation[cite: 697].
* [cite_start]**Junjie Wei:** Orchestration & Metrics implementation[cite: 697].
* [cite_start]**Ye Zhu:** Paper Structuring & Formatting[cite: 697].
* [cite_start]**Cailing Zhong:** Literature Review & Co-author[cite: 697].
