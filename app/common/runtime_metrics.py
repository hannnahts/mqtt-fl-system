import atexit
import json
import os
import resource
import signal
import threading
import time
from pathlib import Path


class RuntimeMetrics:
    def __init__(self):
        self._lock = threading.RLock()
        self._enabled = False
        self._configured = False
        self._metrics_dir = None
        self._output_path = None
        self._role = None
        self._process_label = None
        self._metadata = {}
        self._events = []
        self._published_messages = 0
        self._published_bytes = 0
        self._published_payload_bytes = 0
        self._received_messages = 0
        self._received_bytes = 0
        self._received_payload_bytes = 0
        self._topic_stats = {}
        self._start_epoch_s = 0.0
        self._start_perf_s = 0.0
        self._start_cpu_s = 0.0
        self._original_signal_handlers = {}

    def configure(self, role, process_label=None, metadata=None):
        with self._lock:
            if self._configured:
                return

            metrics_dir = os.getenv("FL_METRICS_DIR")
            self._enabled = bool(metrics_dir)
            self._configured = True
            self._role = role
            self._process_label = process_label or role
            self._metadata = metadata or {}

            if not self._enabled:
                return

            self._metrics_dir = Path(metrics_dir)
            self._metrics_dir.mkdir(parents=True, exist_ok=True)
            self._output_path = self._metrics_dir / f"{self._process_label}.json"
            self._start_epoch_s = time.time()
            self._start_perf_s = time.perf_counter()
            self._start_cpu_s = time.process_time()

            atexit.register(self.flush)
            self._install_signal_handler(signal.SIGINT)
            self._install_signal_handler(signal.SIGTERM)
            self.record_event("process_started")

    def record_event(self, name, **data):
        if not self._enabled:
            return

        event = {
            "name": name,
            "timestamp_epoch_s": time.time(),
        }
        if data:
            event["data"] = data

        with self._lock:
            self._events.append(event)

    def record_publish(self, topic, wire_bytes, encoding="identity", payload_bytes=None):
        if not self._enabled:
            return

        wire_size_bytes = len(wire_bytes)
        original_size_bytes = _payload_size_bytes(payload_bytes, wire_size_bytes)

        with self._lock:
            self._published_messages += 1
            self._published_bytes += wire_size_bytes
            self._published_payload_bytes += original_size_bytes
            topic_stats = self._topic_stats.setdefault(topic, self._empty_topic_stats())
            topic_stats["published_messages"] += 1
            topic_stats["published_bytes"] += wire_size_bytes
            topic_stats["published_payload_bytes"] += original_size_bytes
            topic_stats["published_encoding_counts"][encoding] = (
                topic_stats["published_encoding_counts"].get(encoding, 0) + 1
            )

    def record_receive(self, topic, wire_bytes, encoding="identity", payload_bytes=None):
        if not self._enabled:
            return

        wire_size_bytes = len(wire_bytes)
        original_size_bytes = _payload_size_bytes(payload_bytes, wire_size_bytes)

        with self._lock:
            self._received_messages += 1
            self._received_bytes += wire_size_bytes
            self._received_payload_bytes += original_size_bytes
            topic_stats = self._topic_stats.setdefault(topic, self._empty_topic_stats())
            topic_stats["received_messages"] += 1
            topic_stats["received_bytes"] += wire_size_bytes
            topic_stats["received_payload_bytes"] += original_size_bytes
            topic_stats["received_encoding_counts"][encoding] = (
                topic_stats["received_encoding_counts"].get(encoding, 0) + 1
            )

    def flush(self):
        if not self._enabled:
            return

        snapshot = self._build_snapshot()
        with self._output_path.open("w", encoding="utf-8") as file_obj:
            json.dump(snapshot, file_obj, indent=2, sort_keys=True)

    def _build_snapshot(self):
        with self._lock:
            return {
                "role": self._role,
                "process_label": self._process_label,
                "metadata": self._metadata,
                "start_epoch_s": self._start_epoch_s,
                "end_epoch_s": time.time(),
                "wall_time_s": time.perf_counter() - self._start_perf_s,
                "cpu_time_s": time.process_time() - self._start_cpu_s,
                "max_rss_kb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "published_messages": self._published_messages,
                "published_bytes": self._published_bytes,
                "published_payload_bytes": self._published_payload_bytes,
                "received_messages": self._received_messages,
                "received_bytes": self._received_bytes,
                "received_payload_bytes": self._received_payload_bytes,
                "topic_stats": self._topic_stats,
                "events": list(self._events),
            }

    def _install_signal_handler(self, sig):
        current_handler = signal.getsignal(sig)
        self._original_signal_handlers[sig] = current_handler

        def handler(signum, frame):
            self.record_event("signal_received", signal=signum)
            self.flush()
            raise SystemExit(128 + signum)

        signal.signal(sig, handler)

    @staticmethod
    def _empty_topic_stats():
        return {
            "published_messages": 0,
            "published_bytes": 0,
            "published_payload_bytes": 0,
            "published_encoding_counts": {},
            "received_messages": 0,
            "received_bytes": 0,
            "received_payload_bytes": 0,
            "received_encoding_counts": {},
        }


_RUNTIME_METRICS = RuntimeMetrics()


def configure_metrics(role, process_label=None, metadata=None):
    _RUNTIME_METRICS.configure(role, process_label=process_label, metadata=metadata)


def record_event(name, **data):
    _RUNTIME_METRICS.record_event(name, **data)


def record_publish(topic, wire_bytes, encoding="identity", payload_bytes=None):
    _RUNTIME_METRICS.record_publish(
        topic,
        wire_bytes,
        encoding=encoding,
        payload_bytes=payload_bytes,
    )


def record_receive(topic, wire_bytes, encoding="identity", payload_bytes=None):
    _RUNTIME_METRICS.record_receive(
        topic,
        wire_bytes,
        encoding=encoding,
        payload_bytes=payload_bytes,
    )


def flush_metrics():
    _RUNTIME_METRICS.flush()


def _payload_size_bytes(payload_bytes, fallback):
    if payload_bytes is None:
        return fallback
    if isinstance(payload_bytes, int):
        return payload_bytes
    return len(payload_bytes)
