package com.s19.analytics;

/**
 * Common envelope of the three input topics (incident-events, dispatch-events,
 * consistency-events). Public fields + no-arg constructor => Flink POJO serializer.
 */
public class Event {
    public String eventType = "";
    public String region = "global";
    public String severity = "";
    public String reporterId = "";
    public String incidentId = "";
    public int resourceCount = 0;
    public int volunteerCount = 0;
    public int repaired = 0;
    public long ts = 0L;

    public Event() {}

    @Override
    public String toString() {
        return eventType + "@" + region + "/" + ts;
    }
}
