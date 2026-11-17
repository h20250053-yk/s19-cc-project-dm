"""Alert Service - asynchronous, event-driven serverless function (Cloud Run functions, gen2).

Trigger : Pub/Sub topic `alert-requests` (Eventarc), published by Incident Intake (CRITICAL
          incidents) and Dispatch (assignment notices).
Work    : 1. idempotency check on the Pub/Sub message id (redeliveries are ignored)
          2. resolve recipients: explicitly listed volunteers, or available volunteers in the region
          3. fan out one notification per recipient per channel
          4. persist the alert + delivery report in Firestore `alerts/{messageId}`
Channels: notifications are recorded as structured log entries + Firestore delivery records
          ("simulated" provider). This keeps the project at zero messaging cost; a real SMS/push
          provider would be called inside `deliver()` (single integration point).
Retries : the function raises on transient errors so Pub/Sub redelivers (retry policy set in Terraform).
"""
import base64
import json
import logging
import os
import time

import functions_framework
from google.cloud import firestore

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("alert-service")

PROJECT_ID = os.getenv("GCP_PROJECT_ID") or None
DATABASE = os.getenv("FIRESTORE_DATABASE", "(default)")
MAX_RECIPIENTS = int(os.getenv("MAX_RECIPIENTS", "25"))
CHANNELS_BY_SEVERITY = {"CRITICAL": ["sms", "push", "broadcast"], "HIGH": ["sms", "push"],
                        "MEDIUM": ["push"], "LOW": ["push"]}

_db = None


def db():
    global _db
    if _db is None:
        _db = firestore.Client(project=PROJECT_ID, database=DATABASE)
    return _db


def jlog(severity: str, message: str, **fields):
    # Cloud Run functions forward stdout JSON to Cloud Logging as jsonPayload.
    print(json.dumps({"severity": severity, "message": message, "service": "alert-service", **fields}), flush=True)


def resolve_recipients(alert: dict) -> list:
    ids = alert.get("volunteerIds") or []
    if ids:
        return [{"id": v, "kind": "volunteer"} for v in ids[:MAX_RECIPIENTS]]
    q = (db().collection("volunteers")
         .where(filter=firestore.FieldFilter("region", "==", alert.get("region", "")))
         .where(filter=firestore.FieldFilter("available", "==", True))
         .limit(MAX_RECIPIENTS))
    recipients = [{"id": d.id, "kind": "volunteer"} for d in q.stream()]
    recipients.append({"id": f"region-broadcast-{alert.get('region', 'unknown')}", "kind": "broadcast"})
    return recipients


def deliver(recipient: dict, channel: str, alert: dict) -> dict:
    """Single provider integration point (simulated provider: structured log = delivery)."""
    jlog("NOTICE", "notification delivered", channel=channel, recipient=recipient["id"],
         incidentId=alert.get("incidentId"), alertType=alert.get("alertType"), provider="simulated")
    return {"recipient": recipient["id"], "channel": channel, "status": "DELIVERED", "provider": "simulated",
            "ts": int(time.time() * 1000)}


@functions_framework.cloud_event
def handle_alert(cloud_event):
    message = cloud_event.data.get("message", {})
    message_id = message.get("messageId") or message.get("message_id") or cloud_event["id"]
    try:
        alert = json.loads(base64.b64decode(message.get("data", "")).decode() or "{}")
    except (ValueError, TypeError):
        jlog("ERROR", "malformed alert payload - acknowledged and dropped", messageId=message_id)
        return  # returning normally acks the message: retrying a malformed payload cannot succeed

    ref = db().collection("alerts").document(message_id)
    if ref.get().exists:
        jlog("INFO", "duplicate delivery ignored", messageId=message_id)
        return

    severity = alert.get("severity", "LOW")
    channels = CHANNELS_BY_SEVERITY.get(severity, ["push"])
    started = time.time()
    deliveries = [deliver(r, ch, alert) for r in resolve_recipients(alert) for ch in channels
                  if not (r["kind"] == "broadcast" and ch != "broadcast")]
    ref.set({
        "alertId": message_id, "alertType": alert.get("alertType"), "incidentId": alert.get("incidentId"),
        "region": alert.get("region"), "severity": severity, "message": alert.get("message", ""),
        "channels": channels, "recipientCount": len({d["recipient"] for d in deliveries}),
        "deliveries": deliveries[:100], "createdAt": int(time.time() * 1000),
        "processingMs": int((time.time() - started) * 1000),
    })
    jlog("INFO", "alert processed", messageId=message_id, incidentId=alert.get("incidentId"),
         deliveries=len(deliveries), alertSeverity=severity)
