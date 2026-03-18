import gzip
import json
import paho.mqtt.client as mqtt

from app.common.runtime_metrics import record_publish, record_receive

GZIP_MAGIC = b"FLGZ1"


def create_client(client_id, on_connect=None, on_message=None):
    client = mqtt.Client(client_id=client_id)

    if on_connect:
        client.on_connect = on_connect

    if on_message:
        client.on_message = on_message

    return client


def connect_client(client, host, port):
    client.connect(host, port, 60)


def publish_json(client, topic, payload, encoding="identity"):
    serialized = json.dumps(payload).encode("utf-8")
    wire_payload = encode_payload(serialized, encoding)
    record_publish(
        topic,
        wire_payload,
        encoding=encoding,
        payload_bytes=serialized,
    )
    client.publish(topic, wire_payload)


def parse_json_payload(msg):
    payload, transport = decode_payload(msg.payload)
    record_receive(
        msg.topic,
        msg.payload,
        encoding=transport["encoding"],
        payload_bytes=transport["payload_bytes"],
    )
    return payload, transport


def encode_payload(serialized_payload, encoding):
    if encoding == "gzip":
        return GZIP_MAGIC + gzip.compress(serialized_payload)
    if encoding != "identity":
        raise ValueError(f"Unsupported encoding: {encoding}")
    return serialized_payload


def decode_payload(wire_payload):
    if wire_payload.startswith(GZIP_MAGIC):
        serialized_payload = gzip.decompress(wire_payload[len(GZIP_MAGIC) :])
        return (
            json.loads(serialized_payload.decode("utf-8")),
            {
                "encoding": "gzip",
                "wire_bytes": len(wire_payload),
                "payload_bytes": len(serialized_payload),
            },
        )

    return (
        json.loads(wire_payload.decode("utf-8")),
        {
            "encoding": "identity",
            "wire_bytes": len(wire_payload),
            "payload_bytes": len(wire_payload),
        },
    )
