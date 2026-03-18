#!/usr/bin/env python3
import argparse
import csv
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.common.config import (
    get_broker_config,
    get_channel_encoding,
    get_client_update_route,
    get_compression_config,
    get_round_count,
    get_topic_config,
    get_update_prefix,
    get_update_route_names,
    load_config,
)


SCENARIOS = [
    {
        "name": "baseline_5_clients",
        "topology": "baseline",
        "compose_file": "docker-compose.baseline.yml",
        "base_config": "configs/experiment_baseline.yaml",
        "num_clients": 5,
    },
    {
        "name": "proposed_5_clients",
        "topology": "proposed",
        "compose_file": "docker-compose.proposed.yml",
        "base_config": "configs/experiment_proposed.yaml",
        "num_clients": 5,
    },
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scenarios",
        nargs="*",
        default=[scenario["name"] for scenario in SCENARIOS],
    )
    parser.add_argument(
        "--output-root",
        default=None,
        help="Optional output directory for benchmark artifacts",
    )
    args = parser.parse_args()

    python_bin = REPO_ROOT / "venv" / "bin" / "python"
    if not python_bin.exists():
        python_bin = Path(sys.executable)

    selected = [scenario for scenario in SCENARIOS if scenario["name"] in set(args.scenarios)]
    if not selected:
        raise SystemExit("No matching scenarios selected")

    timestamp = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    output_root = (
        Path(args.output_root)
        if args.output_root
        else REPO_ROOT / "reports" / "performance_suite" / timestamp
    )
    if not output_root.is_absolute():
        output_root = (REPO_ROOT / output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    scenario_rows = []
    communication_rows = []
    computation_rows = []
    process_rows = []

    for scenario in selected:
        print(f"Running scenario: {scenario['name']}")
        result = run_scenario(python_bin, output_root, scenario)
        scenario_rows.append(result["scenario_row"])
        communication_rows.append(result["communication_row"])
        computation_rows.append(result["computation_row"])
        process_rows.extend(result["process_rows"])

    write_csv(
        output_root / "scenario_summary.csv",
        scenario_rows,
        [
            "scenario",
            "topology",
            "app_topology_mode",
            "num_clients",
            "rounds",
            "status",
            "route_validation",
            "compression_validation",
            "validation_message",
            "round_completion_latency_s",
            "messages_published",
            "wire_bytes_published",
            "payload_bytes_published",
            "wire_bytes_saved_ratio",
        ],
    )
    write_csv(
        output_root / "communication_summary.csv",
        communication_rows,
        [
            "scenario",
            "topology",
            "app_topology_mode",
            "num_clients",
            "rounds",
            "compression_enabled",
            "control_payload_bytes",
            "control_wire_bytes",
            "update_payload_bytes_total",
            "update_wire_bytes_total",
            "avg_update_payload_bytes",
            "avg_update_wire_bytes",
            "global_payload_bytes",
            "global_wire_bytes",
            "messages_published",
            "payload_bytes_published",
            "wire_bytes_published",
            "wire_bytes_saved_ratio",
            "uplink_collection_latency_s",
            "aggregation_latency_s",
            "downlink_completion_latency_s",
            "round_completion_latency_s",
            "update_route",
            "global_route",
        ],
    )
    write_csv(
        output_root / "computation_summary.csv",
        computation_rows,
        [
            "scenario",
            "topology",
            "app_topology_mode",
            "num_clients",
            "rounds",
            "avg_client_training_time_s",
            "max_client_training_time_s",
            "avg_client_cpu_time_s",
            "max_client_cpu_time_s",
            "avg_client_max_rss_kb",
            "max_client_max_rss_kb",
            "aggregator_wall_time_s",
            "aggregator_cpu_time_s",
            "aggregator_max_rss_kb",
            "orchestrator_wall_time_s",
            "orchestrator_cpu_time_s",
            "orchestrator_max_rss_kb",
        ],
    )
    write_csv(
        output_root / "process_metrics.csv",
        process_rows,
        [
            "scenario",
            "process_label",
            "role",
            "broker_role",
            "rounds",
            "wall_time_s",
            "cpu_time_s",
            "max_rss_kb",
            "published_messages",
            "published_bytes",
            "published_payload_bytes",
            "received_messages",
            "received_bytes",
            "received_payload_bytes",
            "training_duration_s",
            "aggregation_duration_s",
        ],
    )
    write_markdown_report(output_root, scenario_rows, communication_rows, computation_rows)
    print(f"Benchmark artifacts written to: {output_root}")


def run_scenario(python_bin, output_root, scenario):
    scenario_dir = output_root / scenario["name"]
    logs_dir = scenario_dir / "process_logs"
    metrics_dir = scenario_dir / "metrics"
    splits_dir = scenario_dir / "data_splits"
    scenario_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)
    splits_dir.mkdir(parents=True, exist_ok=True)

    prepare_log = logs_dir / "data_prep.log"
    config_path = scenario_dir / "scenario_config.yaml"

    run_command(
        [str(python_bin), "scripts/prepare_dataset.py"],
        cwd=REPO_ROOT,
        stdout_path=prepare_log,
        stderr_to_stdout=True,
    )
    run_command(
        [
            str(python_bin),
            "scripts/split_dataset.py",
            "--input",
            "data/processed/clean_air.csv",
            "--clients",
            str(scenario["num_clients"]),
            "--output-dir",
            str(splits_dir),
        ],
        cwd=REPO_ROOT,
        stdout_path=prepare_log,
        stderr_to_stdout=True,
        append=True,
    )

    config = load_config(REPO_ROOT / scenario["base_config"])
    config["experiment_name"] = scenario["name"]
    config["num_clients"] = scenario["num_clients"]
    config["data_dir"] = str(splits_dir.resolve())
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    processes = []
    log_handles = []

    try:
        stop_all_compose()
        run_command(
            ["docker", "compose", "-f", scenario["compose_file"], "up", "-d"],
            cwd=REPO_ROOT,
            stdout_path=logs_dir / "docker_up.log",
            stderr_to_stdout=True,
        )
        wait_for_config_brokers(config, timeout_s=20)
        time.sleep(4)

        aggregator_env = build_metrics_env(metrics_dir, scenario["name"], "aggregator")
        aggregator_env["FL_EXIT_AFTER_PUBLISH"] = "1"
        aggregator_process, aggregator_handle = start_process(
            [
                str(python_bin),
                "-m",
                "app.aggregator.aggregator",
                "--config",
                str(config_path),
            ],
            cwd=REPO_ROOT,
            env=aggregator_env,
            log_path=logs_dir / "aggregator.log",
        )
        processes.append(("aggregator", aggregator_process))
        log_handles.append(aggregator_handle)
        ensure_process_started("aggregator", aggregator_process, logs_dir / "aggregator.log")

        for client_id in range(1, scenario["num_clients"] + 1):
            client_env = build_metrics_env(metrics_dir, scenario["name"], f"client_{client_id}")
            client_env["FL_EXIT_AFTER_GLOBAL"] = "1"
            client_process, client_handle = start_process(
                [
                    str(python_bin),
                    "-m",
                    "app.clients.client",
                    "--config",
                    str(config_path),
                    "--client-id",
                    str(client_id),
                ],
                cwd=REPO_ROOT,
                env=client_env,
                log_path=logs_dir / f"client_{client_id}.log",
            )
            processes.append((f"client_{client_id}", client_process))
            log_handles.append(client_handle)
            ensure_process_started(
                f"client_{client_id}",
                client_process,
                logs_dir / f"client_{client_id}.log",
            )

        wait_for_log_text(logs_dir / "aggregator.log", "aggregator connected", timeout_s=30)
        for client_id in range(1, scenario["num_clients"] + 1):
            wait_for_log_text(
                logs_dir / f"client_{client_id}.log",
                "client connected",
                timeout_s=60,
            )

        orchestrator_env = build_metrics_env(metrics_dir, scenario["name"], "orchestrator")
        orchestrator_process, orchestrator_handle = start_process(
            [
                str(python_bin),
                "-m",
                "app.orchestrator.orchestrator",
                "--config",
                str(config_path),
            ],
            cwd=REPO_ROOT,
            env=orchestrator_env,
            log_path=logs_dir / "orchestrator.log",
        )
        processes.append(("orchestrator", orchestrator_process))
        log_handles.append(orchestrator_handle)
        ensure_process_started("orchestrator", orchestrator_process, logs_dir / "orchestrator.log")

        wait_for_exit(orchestrator_process, timeout_s=60)
        for process_name, process in processes:
            if process_name == "orchestrator":
                continue
            wait_for_exit(process, timeout_s=60)

    finally:
        for _, process in processes:
            terminate_process(process)
        for handle in log_handles:
            handle.close()
        run_command(
            ["docker", "compose", "-f", scenario["compose_file"], "down", "--remove-orphans"],
            cwd=REPO_ROOT,
            stdout_path=logs_dir / "docker_down.log",
            stderr_to_stdout=True,
            check=False,
        )

    metrics = load_metrics(metrics_dir)
    validation = validate_metrics(metrics, config, scenario)
    derived = build_derived_metrics(metrics, config, scenario, validation)
    write_json(scenario_dir / "derived_metrics.json", derived)

    return {
        "scenario_row": derived["scenario_row"],
        "communication_row": derived["communication_row"],
        "computation_row": derived["computation_row"],
        "process_rows": derived["process_rows"],
    }


def build_metrics_env(metrics_dir, scenario_name, process_label):
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["FL_METRICS_DIR"] = str(metrics_dir)
    env["FL_PROCESS_LABEL"] = process_label
    env["FL_RUN_NAME"] = scenario_name
    return env


def start_process(cmd, cwd, env, log_path):
    handle = log_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        cmd,
        cwd=cwd,
        env=env,
        stdout=handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return process, handle


def run_command(
    cmd,
    cwd,
    stdout_path=None,
    stderr_to_stdout=False,
    append=False,
    check=True,
):
    stdout_handle = None
    try:
        if stdout_path is not None:
            mode = "a" if append else "w"
            stdout_handle = stdout_path.open(mode, encoding="utf-8")
        result = subprocess.run(
            cmd,
            cwd=cwd,
            stdout=stdout_handle if stdout_handle else subprocess.PIPE,
            stderr=subprocess.STDOUT if stderr_to_stdout else subprocess.PIPE,
            text=True,
            check=False,
        )
    finally:
        if stdout_handle is not None:
            stdout_handle.close()

    if check and result.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")
    return result


def stop_all_compose():
    for compose_file in ("docker-compose.baseline.yml", "docker-compose.proposed.yml"):
        run_command(
            ["docker", "compose", "-f", compose_file, "down", "--remove-orphans"],
            cwd=REPO_ROOT,
            check=False,
        )


def wait_for_exit(process, timeout_s):
    process.wait(timeout=timeout_s)


def wait_for_broker(host, port, timeout_s):
    deadline = time.time() + timeout_s
    last_error = None

    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError as exc:
            last_error = exc
            time.sleep(0.5)

    raise TimeoutError(f"Broker {host}:{port} not ready within {timeout_s}s: {last_error}")


def wait_for_config_brokers(config, timeout_s):
    seen = set()
    broker_entries = config.get("brokers", {})
    if broker_entries:
        brokers = [
            {
                "host": broker_config["host"],
                "port": int(broker_config["port"]),
            }
            for broker_config in broker_entries.values()
        ]
    else:
        brokers = [
            {
                "host": config["broker_host"],
                "port": int(config["broker_port"]),
            }
        ]

    for broker in brokers:
        broker_key = (broker["host"], broker["port"])
        if broker_key in seen:
            continue
        wait_for_broker(broker["host"], broker["port"], timeout_s=timeout_s)
        seen.add(broker_key)


def ensure_process_started(process_name, process, log_path, startup_wait_s=1):
    time.sleep(startup_wait_s)
    if process.poll() is None:
        return

    log_excerpt = ""
    if log_path.exists():
        log_excerpt = log_path.read_text(encoding="utf-8")
    raise RuntimeError(
        f"{process_name} exited early with code {process.returncode}. Log output:\n{log_excerpt}"
    )


def wait_for_log_text(log_path, expected_text, timeout_s):
    deadline = time.time() + timeout_s
    last_text = ""

    while time.time() < deadline:
        if log_path.exists():
            last_text = log_path.read_text(encoding="utf-8")
            if expected_text in last_text:
                return
        time.sleep(0.5)

    raise TimeoutError(
        f"Did not observe '{expected_text}' in {log_path} within {timeout_s}s. "
        f"Last log contents:\n{last_text}"
    )


def terminate_process(process):
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def load_metrics(metrics_dir):
    metrics = {}
    for json_path in metrics_dir.glob("*.json"):
        metrics[json_path.stem] = json.loads(json_path.read_text(encoding="utf-8"))
    return metrics


def validate_metrics(metrics, config, scenario):
    errors = []
    route_errors = []
    compression_errors = []
    warm_start_errors = []

    topics = get_topic_config(config)
    compression = get_compression_config(config)
    rounds = get_round_count(config)
    update_route_names = get_update_route_names(config)
    aggregator = metrics.get("aggregator")
    orchestrator = metrics.get("orchestrator")
    client_metrics = [metrics.get(f"client_{idx}") for idx in range(1, scenario["num_clients"] + 1)]

    if orchestrator is None:
        errors.append("missing orchestrator metrics")
    if aggregator is None:
        errors.append("missing aggregator metrics")

    if orchestrator and len(all_events(orchestrator, "control_published")) != rounds:
        errors.append("orchestrator did not publish the expected number of control messages")
    if aggregator:
        if len(all_events(aggregator, "update_received")) != scenario["num_clients"] * rounds:
            errors.append("aggregator did not receive all client updates across all rounds")
        if len(all_events(aggregator, "global_model_published")) != rounds:
            errors.append("aggregator did not publish the expected number of global models")

    for idx, client_metric in enumerate(client_metrics, start=1):
        if client_metric is None:
            errors.append(f"missing client_{idx} metrics")
            continue
        if len(all_events(client_metric, "training_finished")) != rounds:
            errors.append(f"client_{idx} did not finish training for every round")
        if len(all_events(client_metric, "global_model_received")) != rounds:
            errors.append(f"client_{idx} did not receive a global model for every round")

        if rounds > 1:
            for event in all_events(client_metric, "training_started"):
                round_id = int(event.get("data", {}).get("round", 0))
                if round_id <= 1:
                    continue
                initial_model_round = int(event.get("data", {}).get("initial_model_round", -1))
                if initial_model_round != round_id - 1:
                    warm_start_errors.append(
                        f"client_{idx} did not warm-start round {round_id} from global round {round_id - 1}"
                    )

    if aggregator and orchestrator and all(client_metrics):
        aggregator_role = metric_broker_role(aggregator)
        orchestrator_role = metric_broker_role(orchestrator)
        client_roles = {metric_broker_role(metric) for metric in client_metrics}

        if scenario["topology"] == "baseline":
            if aggregator_role != orchestrator_role or len(client_roles) != 1 or aggregator_role not in client_roles:
                route_errors.append("baseline actors are not on the same broker role")
        else:
            if aggregator_role != "cloud_b":
                route_errors.append("aggregator publish plane is not connected to cloud_b")
            if orchestrator_role != "cloud_b":
                route_errors.append("orchestrator is not connected to cloud_b")
            if client_roles != {"edge"}:
                route_errors.append("one or more clients are not connected to the edge broker")

            if not topics["control_publish_topic"].startswith("fl/cloud/"):
                route_errors.append("control topic is not namespaced under fl/cloud/")
            if not topics["global_publish_topic"].startswith("fl/cloud/"):
                route_errors.append("global publish topic is not namespaced under fl/cloud/")
            for route_name in update_route_names:
                if not get_update_prefix(config, route_name, "publish").startswith("fl/edge/updates/"):
                    route_errors.append(f"update route {route_name} is not namespaced under fl/edge/updates/")

            if not all_event_topics_in_prefixes(
                client_metrics,
                "update_published",
                [get_update_prefix(config, route_name, "publish") for route_name in update_route_names],
            ):
                route_errors.append("client update publish events do not use the expected edge topic paths")
            if not all_event_topics_in_prefixes(
                [aggregator],
                "update_received",
                [get_update_prefix(config, route_name, "subscribe") for route_name in update_route_names],
            ):
                route_errors.append("aggregator update receive events do not use the expected routed topic paths")
            if not all_event_topics_equal([aggregator], "global_model_published", topics["global_publish_topic"]):
                route_errors.append("aggregator did not publish the global model on the expected cloud topic")
            if not all_event_topics_equal(client_metrics, "global_model_received", topics["global_subscribe_topic"]):
                route_errors.append("clients did not receive the global model on the expected topic")
            observed_update_targets = {
                event.get("data", {}).get("route_target")
                for client_metric in client_metrics
                for event in all_events(client_metric, "update_published")
            }
            if set(update_route_names) - observed_update_targets:
                route_errors.append("not all configured cloud update routes were used by clients")

        if compression["enabled"]:
            expected_update_encoding = get_channel_encoding(config, "updates")
            expected_global_encoding = get_channel_encoding(config, "global_model")
            if expected_update_encoding != "identity":
                for idx, client_metric in enumerate(client_metrics, start=1):
                    route_name = get_client_update_route(config, idx)
                    update_prefix = get_update_prefix(config, route_name, "publish")
                    update_topic = f"{update_prefix}/client_{idx}"
                    published_counts = topic_stat(client_metric, update_topic)["published_encoding_counts"]
                    if published_counts.get(expected_update_encoding, 0) == 0:
                        compression_errors.append(
                            f"client_{idx} did not publish updates with {expected_update_encoding}"
                        )
            if expected_global_encoding != "identity":
                global_counts = topic_stat(aggregator, topics["global_publish_topic"])["published_encoding_counts"]
                if global_counts.get(expected_global_encoding, 0) == 0:
                    compression_errors.append(
                        f"aggregator did not publish the global model with {expected_global_encoding}"
                    )
        else:
            compression_errors = []

    route_validation = "passed" if not route_errors else "failed"
    compression_validation = (
        "disabled"
        if not compression["enabled"]
        else ("passed" if not compression_errors else "failed")
    )

    all_errors = errors + route_errors + compression_errors + warm_start_errors
    return {
        "status": "passed" if not all_errors else "failed",
        "message": "; ".join(all_errors) if all_errors else "all validation checks passed",
        "route_validation": route_validation,
        "compression_validation": compression_validation,
    }


def build_derived_metrics(metrics, config, scenario, validation):
    topics = get_topic_config(config)
    compression = get_compression_config(config)
    rounds = get_round_count(config)
    aggregator = metrics["aggregator"]
    orchestrator = metrics["orchestrator"]
    clients = [metrics[f"client_{idx}"] for idx in range(1, scenario["num_clients"] + 1)]

    control_topic = topics["control_publish_topic"]
    global_topic = topics["global_publish_topic"]
    app_topology_mode = describe_app_topology(config, metrics, scenario["num_clients"])

    control_publish_ts = first_event_ts(orchestrator, "control_published")
    update_received_ts = all_event_ts(aggregator, "update_received")
    aggregation_start_ts = first_event_ts(aggregator, "aggregation_started")
    aggregation_finish_ts = last_event_ts(aggregator, "aggregation_finished")
    global_publish_ts = last_event_ts(aggregator, "global_model_published")
    client_global_received_ts = [
        last_event_ts(client_metrics, "global_model_received")
        for client_metrics in clients
    ]

    update_payload_total = sum(
        topic_stat(
            client_metrics,
            f"{get_update_prefix(config, get_client_update_route(config, idx), 'publish')}/client_{idx}",
        )["published_payload_bytes"]
        for idx, client_metrics in enumerate(clients, start=1)
    )
    update_wire_total = sum(
        topic_stat(
            client_metrics,
            f"{get_update_prefix(config, get_client_update_route(config, idx), 'publish')}/client_{idx}",
        )["published_bytes"]
        for idx, client_metrics in enumerate(clients, start=1)
    )
    total_published_messages = sum(metric["published_messages"] for metric in metrics.values())
    total_published_wire_bytes = sum(metric["published_bytes"] for metric in metrics.values())
    total_published_payload_bytes = sum(
        metric.get("published_payload_bytes", metric["published_bytes"])
        for metric in metrics.values()
    )

    client_training_durations = []
    for client_metrics in clients:
        client_training_durations.extend(
            event_durations(client_metrics, "training_started", "training_finished")
        )

    client_cpu_times = [client_metrics["cpu_time_s"] for client_metrics in clients]
    client_max_rss_kb = [client_metrics["max_rss_kb"] for client_metrics in clients]

    control_stats = topic_stat(orchestrator, control_topic)
    global_stats = topic_stat(aggregator, global_topic)
    wire_bytes_saved_ratio = safe_divide(
        total_published_payload_bytes - total_published_wire_bytes,
        total_published_payload_bytes,
    )

    communication_row = {
        "scenario": scenario["name"],
        "topology": scenario["topology"],
        "app_topology_mode": app_topology_mode,
        "num_clients": scenario["num_clients"],
        "rounds": rounds,
        "compression_enabled": compression["enabled"],
        "control_payload_bytes": control_stats["published_payload_bytes"],
        "control_wire_bytes": control_stats["published_bytes"],
        "update_payload_bytes_total": update_payload_total,
        "update_wire_bytes_total": update_wire_total,
        "avg_update_payload_bytes": safe_divide(update_payload_total, scenario["num_clients"] * rounds),
        "avg_update_wire_bytes": safe_divide(update_wire_total, scenario["num_clients"] * rounds),
        "global_payload_bytes": global_stats["published_payload_bytes"],
        "global_wire_bytes": global_stats["published_bytes"],
        "messages_published": total_published_messages,
        "payload_bytes_published": total_published_payload_bytes,
        "wire_bytes_published": total_published_wire_bytes,
        "wire_bytes_saved_ratio": wire_bytes_saved_ratio,
        "uplink_collection_latency_s": duration_from_points(control_publish_ts, max_or_none(update_received_ts)),
        "aggregation_latency_s": duration_from_points(aggregation_start_ts, aggregation_finish_ts),
        "downlink_completion_latency_s": duration_from_points(global_publish_ts, max_or_none(client_global_received_ts)),
        "round_completion_latency_s": duration_from_points(control_publish_ts, max_or_none(client_global_received_ts)),
        "update_route": describe_update_route(config, scenario["topology"], clients),
        "global_route": describe_global_route(scenario["topology"], aggregator, clients),
    }

    computation_row = {
        "scenario": scenario["name"],
        "topology": scenario["topology"],
        "app_topology_mode": app_topology_mode,
        "num_clients": scenario["num_clients"],
        "rounds": rounds,
        "avg_client_training_time_s": rounded_mean(client_training_durations),
        "max_client_training_time_s": rounded_max(client_training_durations),
        "avg_client_cpu_time_s": rounded_mean(client_cpu_times),
        "max_client_cpu_time_s": rounded_max(client_cpu_times),
        "avg_client_max_rss_kb": rounded_mean(client_max_rss_kb),
        "max_client_max_rss_kb": rounded_max(client_max_rss_kb),
        "aggregator_wall_time_s": round(aggregator["wall_time_s"], 6),
        "aggregator_cpu_time_s": round(aggregator["cpu_time_s"], 6),
        "aggregator_max_rss_kb": aggregator["max_rss_kb"],
        "orchestrator_wall_time_s": round(orchestrator["wall_time_s"], 6),
        "orchestrator_cpu_time_s": round(orchestrator["cpu_time_s"], 6),
        "orchestrator_max_rss_kb": orchestrator["max_rss_kb"],
    }

    scenario_row = {
        "scenario": scenario["name"],
        "topology": scenario["topology"],
        "app_topology_mode": app_topology_mode,
        "num_clients": scenario["num_clients"],
        "rounds": rounds,
        "status": validation["status"],
        "route_validation": validation["route_validation"],
        "compression_validation": validation["compression_validation"],
        "validation_message": validation["message"],
        "round_completion_latency_s": communication_row["round_completion_latency_s"],
        "messages_published": total_published_messages,
        "wire_bytes_published": total_published_wire_bytes,
        "payload_bytes_published": total_published_payload_bytes,
        "wire_bytes_saved_ratio": wire_bytes_saved_ratio,
    }

    process_rows = []
    for process_label, process_metrics in sorted(metrics.items()):
        process_rows.append(
            {
                "scenario": scenario["name"],
                "process_label": process_label,
                "role": process_metrics["role"],
                "broker_role": metric_broker_role(process_metrics),
                "rounds": rounds,
                "wall_time_s": round(process_metrics["wall_time_s"], 6),
                "cpu_time_s": round(process_metrics["cpu_time_s"], 6),
                "max_rss_kb": process_metrics["max_rss_kb"],
                "published_messages": process_metrics["published_messages"],
                "published_bytes": process_metrics["published_bytes"],
                "published_payload_bytes": process_metrics.get(
                    "published_payload_bytes",
                    process_metrics["published_bytes"],
                ),
                "received_messages": process_metrics["received_messages"],
                "received_bytes": process_metrics["received_bytes"],
                "received_payload_bytes": process_metrics.get(
                    "received_payload_bytes",
                    process_metrics["received_bytes"],
                ),
                "training_duration_s": total_event_duration(
                    process_metrics,
                    "training_started",
                    "training_finished",
                ),
                "aggregation_duration_s": total_event_duration(
                    process_metrics,
                    "aggregation_started",
                    "aggregation_finished",
                ),
            }
        )

    return {
        "scenario_row": scenario_row,
        "communication_row": communication_row,
        "computation_row": computation_row,
        "process_rows": process_rows,
}


def describe_app_topology(config, metrics, num_clients):
    aggregator_role = metric_broker_role(metrics["aggregator"])
    orchestrator_role = metric_broker_role(metrics["orchestrator"])
    client_roles = {
        metric_broker_role(metrics[f"client_{idx}"])
        for idx in range(1, num_clients + 1)
    }
    if (
        set(get_update_route_names(config)) == {"cloud_a", "cloud_b"}
        and aggregator_role == "cloud_b"
        and orchestrator_role == "cloud_b"
        and client_roles == {"edge"}
    ):
        return "edge_clients_dual_cloud_brokers"
    return "single_broker"


def describe_update_route(config, topology, clients):
    if topology == "proposed" and clients:
        return f"{metric_broker_role(clients[0])}->{'/'.join(get_update_route_names(config))}"
    return "local"


def describe_global_route(topology, aggregator, clients):
    if topology == "proposed" and clients:
        return f"{metric_broker_role(aggregator)}->{metric_broker_role(clients[0])}"
    return "local"


def metric_broker_role(metric):
    return metric.get("metadata", {}).get("broker_role", "unknown")


def first_event(metric, event_name):
    for event in metric.get("events", []):
        if event["name"] == event_name:
            return event
    return None


def all_events(metric, event_name):
    return [
        event
        for event in metric.get("events", [])
        if event["name"] == event_name
    ]


def first_event_ts(metric, event_name):
    event = first_event(metric, event_name)
    return None if event is None else event["timestamp_epoch_s"]


def all_event_ts(metric, event_name):
    return [event["timestamp_epoch_s"] for event in all_events(metric, event_name)]


def last_event_ts(metric, event_name):
    event_times = all_event_ts(metric, event_name)
    return None if not event_times else event_times[-1]


def event_durations(metric, start_name, finish_name):
    start_events = all_events(metric, start_name)
    finish_events = all_events(metric, finish_name)
    durations = []
    for start_event, finish_event in zip(start_events, finish_events):
        durations.append(round(finish_event["timestamp_epoch_s"] - start_event["timestamp_epoch_s"], 6))
    return durations


def total_event_duration(metric, start_name, finish_name):
    durations = event_durations(metric, start_name, finish_name)
    if not durations:
        return None
    return round(sum(durations), 6)


def all_event_topics_start_with(metrics, event_name, prefix):
    for metric in metrics:
        for event in all_events(metric, event_name):
            topic = event.get("data", {}).get("topic", "")
            if not topic.startswith(prefix):
                return False
    return True


def all_event_topics_in_prefixes(metrics, event_name, prefixes):
    for metric in metrics:
        for event in all_events(metric, event_name):
            topic = event.get("data", {}).get("topic", "")
            if not any(topic.startswith(prefix) for prefix in prefixes):
                return False
    return True


def all_event_topics_equal(metrics, event_name, expected_topic):
    for metric in metrics:
        events = all_events(metric, event_name)
        if not events:
            return False
        for event in events:
            if event.get("data", {}).get("topic") != expected_topic:
                return False
    return True


def topic_stat(metric, topic):
    return metric.get("topic_stats", {}).get(
        topic,
        {
            "published_messages": 0,
            "published_bytes": 0,
            "published_payload_bytes": 0,
            "published_encoding_counts": {},
            "received_messages": 0,
            "received_bytes": 0,
            "received_payload_bytes": 0,
            "received_encoding_counts": {},
        },
    )


def duration_from_points(start_ts, end_ts):
    if start_ts is None or end_ts is None:
        return None
    return round(end_ts - start_ts, 6)


def max_or_none(values):
    filtered = [value for value in values if value is not None]
    if not filtered:
        return None
    return max(filtered)


def rounded_mean(values):
    filtered = [value for value in values if value is not None]
    if not filtered:
        return None
    return round(sum(filtered) / len(filtered), 6)


def rounded_max(values):
    filtered = [value for value in values if value is not None]
    if not filtered:
        return None
    return round(max(filtered), 6)


def safe_divide(numerator, denominator):
    if not denominator:
        return None
    return round(numerator / denominator, 6)


def write_csv(path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_json(path, payload):
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_markdown_report(output_root, scenario_rows, communication_rows, computation_rows):
    report = [
        "# 闭环项目通信效率与计算资源测试报告",
        "",
        "## 测试套件",
        "",
        "- `baseline_5_clients`：基线拓扑，5 个客户端，按配置轮次训练",
        "- `proposed_5_clients`：边缘/云拓扑，5 个客户端，按配置轮次训练，云侧调度 + 双云 broker + gzip 通信压缩",
        "",
        "## 场景总览",
        "",
        markdown_table(
            scenario_rows,
            [
                "scenario",
                "app_topology_mode",
                "status",
                "route_validation",
                "compression_validation",
                "round_completion_latency_s",
                "wire_bytes_published",
                "wire_bytes_saved_ratio",
            ],
        ),
        "",
        "## 通信效率",
        "",
        markdown_table(
            communication_rows,
            [
                "scenario",
                "update_payload_bytes_total",
                "update_wire_bytes_total",
                "global_payload_bytes",
                "global_wire_bytes",
                "update_route",
                "global_route",
                "round_completion_latency_s",
            ],
        ),
        "",
        "## 计算资源",
        "",
        markdown_table(
            computation_rows,
            [
                "scenario",
                "avg_client_training_time_s",
                "max_client_training_time_s",
                "avg_client_cpu_time_s",
                "avg_client_max_rss_kb",
                "aggregator_cpu_time_s",
                "aggregator_max_rss_kb",
            ],
        ),
        "",
        "## 结果文件",
        "",
        "- `scenario_summary.csv`",
        "- `communication_summary.csv`",
        "- `computation_summary.csv`",
        "- `process_metrics.csv`",
    ]
    (output_root / "benchmark_report.md").write_text("\n".join(report), encoding="utf-8")


def markdown_table(rows, columns):
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join(["---"] * len(columns)) + " |"
    lines = [header, separator]
    for row in rows:
        cells = []
        for column in columns:
            value = row.get(column, "")
            cells.append("" if value is None else str(value))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


if __name__ == "__main__":
    main()
