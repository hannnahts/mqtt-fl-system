import argparse
import os

from app.common.config import (
    get_broker_config,
    get_channel_encoding,
    get_client_update_route,
    get_round_count,
    get_topic_config,
    get_training_config,
    get_update_prefix,
    load_config,
)
from app.common.model_utils import train_local_model
from app.common.mqtt_utils import connect_client, create_client, parse_json_payload, publish_json
from app.common.runtime_metrics import configure_metrics, record_event


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--client-id", required=True)
    args = parser.parse_args()

    config = load_config(args.config)
    broker = get_broker_config(config, "client")
    topics = get_topic_config(config)
    training = get_training_config(config)
    max_rounds = get_round_count(config)
    host = broker["host"]
    port = broker["port"]
    control_topic = topics["control_subscribe_topic"]
    global_topic = topics["global_subscribe_topic"]
    route_target = get_client_update_route(config, args.client_id)
    update_prefix = get_update_prefix(config, route_target, "publish")
    update_topic = f"{update_prefix}/client_{args.client_id}"
    data_file = f"{config['data_dir']}/client_{args.client_id}.csv"
    update_encoding = get_channel_encoding(config, "updates")

    current_global_model = None
    started_rounds = set()
    completed_rounds = set()
    pending_rounds = set()

    configure_metrics(
        "client",
        process_label=f"client_{args.client_id}",
        metadata={
            "client_id": args.client_id,
            "config_path": args.config,
            "broker_host": host,
            "broker_port": port,
            "broker_role": broker["role"],
            "data_file": data_file,
            "control_topic": control_topic,
            "update_topic": update_topic,
            "global_topic": global_topic,
            "update_encoding": update_encoding,
            "route_target": route_target,
            "rounds": max_rounds,
        },
    )

    def maybe_start_round(client, round_id):
        nonlocal current_global_model

        if round_id in started_rounds:
            return

        if training["reuse_global_model"] and round_id > 1:
            if current_global_model is None or current_global_model.get("round") != round_id - 1:
                pending_rounds.add(round_id)
                record_event(
                    "control_pending",
                    round=round_id,
                    expected_global_round=round_id - 1,
                    available_global_round=(
                        None if current_global_model is None else current_global_model.get("round")
                    ),
                )
                return

        started_rounds.add(round_id)
        warm_start_model = current_global_model if training["reuse_global_model"] else None
        print("training local model")
        record_event(
            "training_started",
            round=round_id,
            data_file=data_file,
            route_target=route_target,
            initial_model_round=(
                0 if warm_start_model is None else int(warm_start_model.get("round", 0))
            ),
        )

        weights, bias, initial_round = train_local_model(
            data_file,
            initial_model=warm_start_model,
            local_epochs=training["local_epochs"],
            learning_rate=training["learning_rate"],
        )
        record_event(
            "training_finished",
            round=round_id,
            weight_count=len(weights),
            initial_model_round=initial_round,
        )

        update = {
            "client_id": args.client_id,
            "weights": weights,
            "bias": bias,
            "round": round_id,
            "route_target": route_target,
            "initial_model_round": initial_round,
        }

        publish_json(client, update_topic, update, encoding=update_encoding)
        record_event(
            "update_published",
            round=round_id,
            topic=update_topic,
            broker_role=broker["role"],
            route_target=route_target,
            encoding=update_encoding,
            initial_model_round=initial_round,
            weight_count=len(weights),
        )

    def drain_pending_rounds(client):
        progressed = True
        while progressed:
            progressed = False
            for round_id in sorted(pending_rounds):
                if current_global_model is not None and current_global_model.get("round") == round_id - 1:
                    pending_rounds.remove(round_id)
                    maybe_start_round(client, round_id)
                    progressed = True
                    break

    def on_connect(client, userdata, flags, rc):
        print("client connected")
        record_event(
            "mqtt_connected",
            rc=rc,
            broker_role=broker["role"],
            control_topic=control_topic,
            global_topic=global_topic,
            route_target=route_target,
        )

        client.subscribe(control_topic)
        client.subscribe(global_topic)

    def on_message(client, userdata, msg):
        nonlocal current_global_model

        payload, transport = parse_json_payload(msg)

        if msg.topic == control_topic:
            round_id = int(payload["round"])
            record_event(
                "control_received",
                round=round_id,
                topic=msg.topic,
                broker_role=broker["role"],
                encoding=transport["encoding"],
                wire_bytes=transport["wire_bytes"],
                payload_bytes=transport["payload_bytes"],
            )

            if payload.get("command") == "start_round":
                maybe_start_round(client, round_id)

        if msg.topic == global_topic:
            round_id = int(payload.get("round", 0))
            if current_global_model is None or round_id >= int(current_global_model.get("round", 0)):
                current_global_model = {
                    "weights": payload.get("weights", []),
                    "bias": payload.get("bias", 0.0),
                    "round": round_id,
                }
            completed_rounds.add(round_id)

            record_event(
                "global_model_received",
                topic=msg.topic,
                broker_role=broker["role"],
                encoding=transport["encoding"],
                wire_bytes=transport["wire_bytes"],
                payload_bytes=transport["payload_bytes"],
                round=round_id,
                weight_count=len(payload.get("weights", [])),
            )

            print("received global model")
            print(payload)
            drain_pending_rounds(client)

            if round_id >= max_rounds and os.getenv("FL_EXIT_AFTER_GLOBAL") == "1":
                record_event(
                    "disconnect_requested",
                    reason="final_global_model_received",
                    round=round_id,
                )
                client.disconnect()

    client = create_client(f"client_{args.client_id}", on_connect, on_message)
    connect_client(client, host, port)
    client.loop_forever()


if __name__ == "__main__":
    main()
