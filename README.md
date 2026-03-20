# Communication-Efficient Federated Learning in MQTT-based Edge-Cloud Systems

This repository contains the source code for a hierarchical multi-broker federated learning (FL) system operating over the Message Queuing Telemetry Transport (MQTT) protocol. This system is designed to address communication overhead and single-broker congestion issues inherent in traditional centralized MQTT-based FL frameworks for resource-constrained IoT environments.

The prototype demonstrates a practical application using a real-world air quality dataset from Newcastle to collaboratively train a linear regression model without exposing raw edge data to the cloud.

## System Architecture

The core innovation of this project is a decoupled, hierarchical edge-cloud topology that migrates from a baseline single-broker architecture to a tiered multi-broker design.

### Structural Overview

The system is structured across three primary logical layers:

1.  **Edge Layer:** Emulates multiple distributed clients, each maintaining a local data shard. Edge devices are isolated from cloud-side network volatility by connecting exclusively to an Edge-Local Broker.
2.  **Broker Layer:** Forms the communication backbone, divided into one Edge Broker and two parallel Cloud Brokers (Cloud-A and Cloud-B). The Edge Broker acts as a transparent bridge, distributing upstream client updates across Cloud-A and Cloud-B to prevent single-point congestion.
3.  **Cloud Layer:** Hosts the Orchestrator, which manages training rounds via control messages, and the Aggregator, which collects model updates from both cloud paths and disseminates the global model through Cloud-B.

The detailed logical structure and communication paths are illustrated in the diagram below:
