import argparse
import os
import threading
import time

from app.common.config import (
    get_aggregator_publish_broker,
    get_aggregator_update_routes,
    get_channel_encoding,
    get_round_count,
    get_topic_config,
    load_config,
)
from app.common.mqtt_utils import connect_client, create_client, parse_json_payload, publish_json
from app.common.runtime_metrics import configure_metrics, record_event


def federated_average(updates):
    weights = [update["weights"] for update in updates]
    bias = [update["bias"] for update in updates]

    avg_weights = [
        sum(weight_vector[idx] for weight_vector in weights) / len(weights)
        for idx in range(len(weights[0]))
    ]
    avg_bias = sum(bias) / len(bias)
    return avg_weights, avg_bias


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    config = load_config(args.config)
    topics = get_topic_config(config)
    update_routes = get_aggregator_update_routes(config)
    publish_broker = get_aggregator_publish_broker(config)
    num_clients = int(config["num_clients"])
    max_rounds = get_round_count(config)
    global_topic = topics["global_publish_topic"]
    global_encoding = get_channel_encoding(config, "global_model")

    route_clients = {}
    updates_by_round = {}
    completed_rounds = set()
    state_lock = threading.RLock()
    stop_event = threading.Event()

    configure_metrics(
        "aggregator",
        process_label="aggregator",
        metadata={
            "config_path": args.config,
            "broker_role": publish_broker["role"],
            "num_clients": num_clients,
            "rounds": max_rounds,
            "published_global_topic": global_topic,
            "global_encoding": global_encoding,
            "subscribed_update_routes": [
                {
                    "route_name": route["route_name"],
                    "broker_role": route["broker"]["role"],
                    "subscribe_prefix": route["subscribe_prefix"],
                }
                for route in update_routes
            ],
        },
    )

    def maybe_publish_global(route_client, round_id):
        with state_lock:
            round_updates = updates_by_round.get(round_id, {})
            if round_id in completed_rounds or len(round_updates) != num_clients:
                return

            ordered_updates = [round_updates[client_id] for client_id in sorted(round_updates)]
            completed_rounds.add(round_id)

        record_event(
            "aggregation_started",
            round=round_id,
            update_count=len(ordered_updates),
        )
        weights, bias = federated_average(ordered_updates)
        record_event(
            "aggregation_finished",
            round=round_id,
            weight_count=len(weights),
        )

        global_model = {
            "weights": weights,
            "bias": bias,
            "round": round_id,
        }

        publish_json(route_client, global_topic, global_model, encoding=global_encoding)
        record_event(
            "global_model_published",
            round=round_id,
            topic=global_topic,
            broker_role=publish_broker["role"],
            encoding=global_encoding,
            weight_count=len(weights),
        )
        print("global model published")

        if round_id >= max_rounds and os.getenv("FL_EXIT_AFTER_PUBLISH") == "1":
            record_event(
                "disconnect_requested",
                reason="final_global_model_published",
                round=round_id,
            )
            threading.Timer(0.5, stop_event.set).start()

    def build_on_connect(route_name, broker, subscribe_prefix):
        def on_connect(client, userdata, flags, rc):
            print("aggregator connected")
            record_event(
                "mqtt_connected",
                rc=rc,
                broker_role=broker["role"],
                route_name=route_name,
                subscribed_topic=f"{subscribe_prefix}/#",
            )
            client.subscribe(f"{subscribe_prefix}/#")

        return on_connect

    def build_on_message(route_name, broker):
        def on_message(client, userdata, msg):
            payload, transport = parse_json_payload(msg)
            round_id = int(payload.get("round", 0))
            client_id = str(payload.get("client_id"))

            with state_lock:
                round_updates = updates_by_round.setdefault(round_id, {})
                round_updates[client_id] = payload
                pending_updates = len(round_updates)

            print("received update")
            record_event(
                "update_received",
                client_id=client_id,
                round=round_id,
                topic=msg.topic,
                broker_role=broker["role"],
                route_name=route_name,
                route_target=payload.get("route_target", route_name),
                encoding=transport["encoding"],
                wire_bytes=transport["wire_bytes"],
                payload_bytes=transport["payload_bytes"],
                pending_updates=pending_updates,
            )

            maybe_publish_global(route_clients[publish_broker["role"]], round_id)

        return on_message

    for route in update_routes:
        broker = route["broker"]
        route_name = route["route_name"]
        client = create_client(
            f"aggregator_{route_name}",
            on_connect=build_on_connect(route_name, broker, route["subscribe_prefix"]),
            on_message=build_on_message(route_name, broker),
        )
        connect_client(client, broker["host"], broker["port"])
        route_clients[broker["role"]] = client

    publish_client = route_clients.get(publish_broker["role"])
    if publish_client is None:
        publish_client = create_client("aggregator_publish")
        connect_client(publish_client, publish_broker["host"], publish_broker["port"])
        route_clients[publish_broker["role"]] = publish_client

    for client in route_clients.values():
        client.loop_start()

    try:
        while not stop_event.is_set():
            time.sleep(0.1)
    finally:
        for client in route_clients.values():
            client.disconnect()
        for client in route_clients.values():
            client.loop_stop()


if __name__ == "__main__":
    main()
