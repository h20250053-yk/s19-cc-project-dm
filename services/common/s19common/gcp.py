"""Google Cloud helpers shared by the services.

Authentication: on GKE every pod runs as Kubernetes SA `s19-app`, which Workload
Identity maps to the Google SA `s19-workload@PROJECT.iam.gserviceaccount.com`
(created by Terraform). The client libraries pick the credentials up automatically.
Locally, the emulators are used when *_EMULATOR_HOST variables are set
(see docker-compose.yml).
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("gcp")

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")


class PubSubPublisher:
    def __init__(self, project: str, topic: str):
        self.enabled = bool(project and topic)
        self.topic_path = None
        if self.enabled:
            from google.cloud import pubsub_v1
            self._client = pubsub_v1.PublisherClient()
            self.topic_path = self._client.topic_path(project, topic)
            log.info("pubsub publisher ready", extra={"topic": self.topic_path})
        else:
            log.warning("Pub/Sub disabled (GCP_PROJECT_ID or topic missing)")

    def publish(self, message: dict, **attributes: str) -> Optional[str]:
        if not self.enabled:
            return None
        try:
            fut = self._client.publish(self.topic_path, json.dumps(message).encode(),
                                       **{k: str(v) for k, v in attributes.items()})
            return fut.result(timeout=10)
        except Exception:  # noqa: BLE001
            log.exception("pubsub publish failed", extra={"topic": self.topic_path})
            return None


class GcsBatchWriter:
    """Buffers raw records and writes them as NDJSON objects to GCS.

    Batching keeps Class-A operations (the billed write ops) low during load tests:
    one object per FLUSH_SECONDS or per MAX_RECORDS instead of one per request.
    """

    def __init__(self, bucket: str, prefix: str, max_records: int = 500, flush_seconds: float = 30.0):
        self.enabled = bool(bucket)
        self.prefix = prefix.strip("/")
        self.max_records = max_records
        self.flush_seconds = flush_seconds
        self._buf: list[str] = []
        self._lock = threading.Lock()
        self._last = time.time()
        self.objects_written = 0
        if self.enabled:
            from google.cloud import storage
            self._bucket = storage.Client().bucket(bucket)
            threading.Thread(target=self._loop, daemon=True, name="gcs-batch").start()
            log.info("gcs batch writer ready", extra={"bucket": bucket, "prefix": self.prefix})
        else:
            log.warning("GCS raw archive disabled (bucket not set)")

    def add(self, record: dict) -> None:
        if not self.enabled:
            return
        with self._lock:
            self._buf.append(json.dumps(record, separators=(",", ":")))
            full = len(self._buf) >= self.max_records
        if full:
            threading.Thread(target=self.flush, daemon=True).start()

    def _loop(self):
        while True:
            time.sleep(2)
            if time.time() - self._last >= self.flush_seconds:
                self.flush()

    def flush(self) -> Optional[str]:
        with self._lock:
            lines, self._buf = self._buf, []
            self._last = time.time()
        if not lines:
            return None
        now = datetime.now(timezone.utc)
        name = (f"{self.prefix}/{now:%Y/%m/%d/%H}/batch-{os.getenv('HOSTNAME', 'local')}-"
                f"{now:%M%S}-{uuid.uuid4().hex[:6]}.ndjson")
        try:
            self._bucket.blob(name).upload_from_string("\n".join(lines) + "\n",
                                                       content_type="application/x-ndjson")
            self.objects_written += 1
            return name
        except Exception:  # noqa: BLE001
            log.exception("gcs batch upload failed, re-queueing", extra={"records": len(lines)})
            with self._lock:
                self._buf = lines + self._buf
            return None


def upload_bytes(bucket: str, name: str, data: bytes, content_type: str) -> str:
    from google.cloud import storage
    blob = storage.Client().bucket(bucket).blob(name)
    blob.upload_from_string(data, content_type=content_type)
    return f"gs://{bucket}/{name}"


def firestore_client():
    from google.cloud import firestore
    return firestore.Client(project=PROJECT_ID or None, database=os.getenv("FIRESTORE_DATABASE", "(default)"))
