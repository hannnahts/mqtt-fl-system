import argparse
import threading
import time

from app.common.config import get_broker_config, get_channel_encoding, get_round_count, get_topic_config, load_config
from app.common.mqtt_utils import connect_client, create_client, parse_json_payload, publish_json
from app.common.runtime_metrics import configure_metrics, record_event


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--rounds", type=int, default=None)
    args = parser.parse_args()

    config = load_config(args.config)
    broker = get_broker_config(config, "orchestrator")
    topics = get_topic_config(config)
    host = broker["host"]
    port = broker["port"]
    control_topic = topics["control_publish_topic"]
    global_topic = topics["global_subscribe_topic"]
    total_rounds = get_round_count(config, override_rounds=args.rounds)
    control_encoding = get_channel_encoding(config, "control")
    done_event = threading.Event()
    published_rounds = set()
    completed_rounds = set()

    configure_metrics(
        "orchestrator",
        process_label="orchestrator",
        metadata={
            "config_path": args.config,
            "broker_host": host,
            "broker_port": port,
            "broker_role": broker["role"],
            "control_topic": control_topic,
            "global_topic": global_topic,
            "control_encoding": control_encoding,
            "rounds": total_rounds,
        },
    )

    def publish_control(client, round_id):
        if round_id in published_rounds:
            return

        message = {
            "command": "start_round",
            "round": round_id,
            "total_rounds": total_rounds,
        }
        publish_json(client, control_topic, message, encoding=control_encoding)
        record_event(
            "control_published",
            round=round_id,
            topic=control_topic,
            broker_role=broker["role"],
            encoding=control_encoding,
        )
        published_rounds.add(round_id)
        print(f"round {round_id} started")

    def on_connect(client, userdata, flags, rc):
        record_event(
            "mqtt_connected",
            rc=rc,
            broker_role=broker["role"],
            global_topic=global_topic,
        )
        client.subscribe(global_topic)

    def on_message(client, userdata, msg):
        payload, transport = parse_json_payload(msg)
        round_id = int(payload.get("round", 0))
        if round_id in completed_rounds:
            return

        completed_rounds.add(round_id)
        record_event(
            "global_model_observed",
            round=round_id,
            topic=msg.topic,
            broker_role=broker["role"],
            encoding=transport["encoding"],
            wire_bytes=transport["wire_bytes"],
            payload_bytes=transport["payload_bytes"],
        )

        if round_id >= total_rounds:
            done_event.set()
            return

        time.sleep(0.2)
        publish_control(client, round_id + 1)

    client = create_client(
        "orchestrator",
        on_connect=on_connect,
        on_message=on_message,
    )
    connect_client(client, host, port)
    client.loop_start()

    try:
        time.sleep(3)
        publish_control(client, 1)
        done_event.wait(timeout=max(30, total_rounds * 30))
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
