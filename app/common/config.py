import copy

import yaml


def load_config(path):
    with open(path, "r", encoding="utf-8") as file_obj:
        return yaml.safe_load(file_obj)


def get_broker_config(config, actor):
    brokers = config.get("brokers", {})
    broker = brokers.get(actor)

    if broker:
        return {
            "host": broker["host"],
            "port": int(broker["port"]),
            "role": broker.get("role", actor),
        }

    return {
        "host": config["broker_host"],
        "port": int(config["broker_port"]),
        "role": config.get("broker_role", "default"),
    }


def get_topic_config(config):
    topics = config.get("topics", {})
    updates = topics.get("updates", {})
    global_model = topics.get("global_model", {})
    control = topics.get("control", {})
    if isinstance(control, dict):
        control_publish_topic = control.get(
            "publish_topic",
            config.get("control_topic"),
        )
        control_subscribe_topic = control.get(
            "subscribe_topic",
            control_publish_topic,
        )
    else:
        control_publish_topic = control if control else config["control_topic"]
        control_subscribe_topic = control_publish_topic

    return {
        "control": control_subscribe_topic,
        "control_publish_topic": control_publish_topic,
        "control_subscribe_topic": control_subscribe_topic,
        "client_update_publish_prefix": (
            updates["publish_prefix"]
            if "publish_prefix" in updates
            else config.get("client_topic_prefix")
        ),
        "aggregator_update_subscribe_prefix": (
            updates["subscribe_prefix"]
            if "subscribe_prefix" in updates
            else config.get("client_topic_prefix")
        ),
        "global_publish_topic": (
            global_model["publish_topic"]
            if "publish_topic" in global_model
            else config.get("global_topic")
        ),
        "global_subscribe_topic": (
            global_model["subscribe_topic"]
            if "subscribe_topic" in global_model
            else config.get("global_topic")
        ),
        "round_complete_topic": topics.get("round_complete"),
    }


def get_compression_config(config):
    compression = copy.deepcopy(config.get("compression", {}))
    enabled = bool(compression.get("enabled", False))
    channels = compression.get("channels", {})

    return {
        "enabled": enabled,
        "algorithm": compression.get("algorithm", "identity" if not enabled else "gzip"),
        "channels": {
            "control": bool(channels.get("control", False)),
            "updates": bool(channels.get("updates", False)),
            "global_model": bool(channels.get("global_model", False)),
        },
    }


def get_channel_encoding(config, channel):
    compression = get_compression_config(config)
    if not compression["enabled"]:
        return "identity"
    if compression["channels"].get(channel):
        return compression["algorithm"]
    return "identity"


def get_round_count(config, override_rounds=None):
    if override_rounds is not None:
        return int(override_rounds)
    return int(config.get("rounds", 1))


def get_training_config(config):
    training = config.get("training", {})
    return {
        "local_epochs": int(training.get("local_epochs", 5)),
        "learning_rate": float(training.get("learning_rate", 0.0002)),
        "reuse_global_model": bool(training.get("reuse_global_model", True)),
    }


def get_update_route_names(config):
    routing = config.get("routing", {})
    route_names = routing.get("update_broker_order")
    if route_names:
        return list(route_names)

    updates = config.get("topics", {}).get("updates", {})
    publish_prefixes = updates.get("publish_prefixes", {})
    if publish_prefixes:
        return list(publish_prefixes.keys())

    return ["default"]


def get_client_update_route(config, client_id):
    route_names = get_update_route_names(config)
    if not route_names:
        return "default"
    return route_names[(int(client_id) - 1) % len(route_names)]


def get_update_prefix(config, route_name, mode):
    topics = config.get("topics", {})
    updates = topics.get("updates", {})

    if mode == "publish":
        route_prefixes = updates.get("publish_prefixes", {})
        single_prefix = updates.get("publish_prefix") or config.get("client_topic_prefix")
    elif mode == "subscribe":
        route_prefixes = updates.get("subscribe_prefixes", {})
        single_prefix = updates.get("subscribe_prefix") or config.get("client_topic_prefix")
    else:
        raise ValueError(f"Unsupported update prefix mode: {mode}")

    if route_name in route_prefixes:
        return route_prefixes[route_name]
    return single_prefix


def get_aggregator_update_routes(config):
    route_descriptors = []
    for route_name in get_update_route_names(config):
        broker_actor = route_name if route_name != "default" else "aggregator"
        route_descriptors.append(
            {
                "route_name": route_name,
                "broker": get_broker_config(config, broker_actor),
                "subscribe_prefix": get_update_prefix(config, route_name, "subscribe"),
            }
        )
    return route_descriptors


def get_aggregator_publish_broker(config):
    brokers = config.get("brokers", {})
    if "aggregator_publish" in brokers:
        return get_broker_config(config, "aggregator_publish")
    if "aggregator" in brokers:
        return get_broker_config(config, "aggregator")
    first_route = get_update_route_names(config)[0]
    return get_broker_config(config, first_route)
