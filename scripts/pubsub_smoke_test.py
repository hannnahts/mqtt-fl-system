#!/usr/bin/env python3
import argparse
import sys
import threading
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.common.mqtt_utils import connect_client, create_client


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11883)
    parser.add_argument("--topic", default="smoke/test")
    parser.add_argument("--message", default="ping")
    parser.add_argument("--timeout", type=float, default=5.0)
    args = parser.parse_args()

    connected = threading.Event()
    received = threading.Event()
    payload_box = {"message": None}

    def on_connect(client, userdata, flags, rc):
        if rc != 0:
            raise RuntimeError(f"Subscriber connect failed with rc={rc}")
        client.subscribe(args.topic)
        connected.set()

    def on_message(client, userdata, msg):
        payload_box["message"] = msg.payload.decode()
        received.set()
        client.disconnect()

    subscriber = create_client(
        client_id=f"smoke_sub_{int(time.time() * 1000)}",
        on_connect=on_connect,
        on_message=on_message,
    )
    connect_client(subscriber, args.host, args.port)
    subscriber.loop_start()

    try:
        if not connected.wait(args.timeout):
            raise TimeoutError("Subscriber did not connect and subscribe in time")

        publisher = create_client(client_id=f"smoke_pub_{int(time.time() * 1000)}")
        connect_client(publisher, args.host, args.port)
        publisher.loop_start()

        try:
            info = publisher.publish(args.topic, args.message)
            info.wait_for_publish()

            if not received.wait(args.timeout):
                raise TimeoutError("Subscriber did not receive the published message in time")

            if payload_box["message"] != args.message:
                raise ValueError(
                    f"Unexpected payload: {payload_box['message']!r} != {args.message!r}"
                )
        finally:
            publisher.loop_stop()
            publisher.disconnect()
    finally:
        subscriber.loop_stop()
        subscriber.disconnect()

    print(
        f"Pub/sub smoke test passed on {args.host}:{args.port} "
        f"topic={args.topic} payload={args.message}"
    )


if __name__ == "__main__":
    main()
