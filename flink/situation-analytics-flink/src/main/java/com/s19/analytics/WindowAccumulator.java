package com.s19.analytics;

import java.util.HashSet;

/** Per-region, per-window running aggregate (lives in Flink managed window state). */
public class WindowAccumulator {
    public long incidents;
    public long critical;
    public long updates;
    public HashSet<String> reporters = new HashSet<>();
    public long dispatches;
    public long resourcesDispatched;
    public long volunteersDispatched;
    public long resolved;
    public long conflicts;
    public long readRepairs;
    public long quorumFailures;
    public long antiEntropyRepairs;

    public WindowAccumulator() {}

    public void add(Event e) {
        switch (e.eventType) {
            case "INCIDENT_REPORTED":
                incidents++;
                if ("CRITICAL".equals(e.severity)) {
                    critical++;
                }
                if (e.reporterId != null && !e.reporterId.isEmpty()) {
                    reporters.add(e.reporterId);
                }
                break;
            case "INCIDENT_UPDATED":
                updates++;
                break;
            case "DISPATCH_CREATED":
                dispatches++;
                resourcesDispatched += e.resourceCount;
                volunteersDispatched += e.volunteerCount;
                break;
            case "DISPATCH_COMPLETED":
                resolved++;
                break;
            case "CONFLICT_DETECTED":
                conflicts++;
                break;
            case "READ_REPAIR":
                readRepairs++;
                break;
            case "QUORUM_FAILURE":
                quorumFailures++;
                break;
            case "ANTI_ENTROPY_REPAIR":
                antiEntropyRepairs += Math.max(1, e.repaired);
                break;
            default:
                break;
        }
    }

    public WindowAccumulator merge(WindowAccumulator o) {
        incidents += o.incidents;
        critical += o.critical;
        updates += o.updates;
        reporters.addAll(o.reporters);
        dispatches += o.dispatches;
        resourcesDispatched += o.resourcesDispatched;
        volunteersDispatched += o.volunteersDispatched;
        resolved += o.resolved;
        conflicts += o.conflicts;
        readRepairs += o.readRepairs;
        quorumFailures += o.quorumFailures;
        antiEntropyRepairs += o.antiEntropyRepairs;
        return this;
    }
}
