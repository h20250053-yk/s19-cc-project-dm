package com.s19.analytics;

/** One closed 1-minute window for one region; serialised to JSON on the results topic. */
public class WindowResult {
    public String region;
    public long windowStart;
    public long windowEnd;
    public long incidents;
    public long critical;
    public long updates;
    public long uniqueReporters;
    public long dispatches;
    public long resourcesDispatched;
    public long volunteersDispatched;
    public long resolved;
    public long conflicts;
    public long readRepairs;
    public long quorumFailures;
    public long antiEntropyRepairs;
    // filled in by SurgeDetector (keyed state across windows)
    public long prevIncidents;
    public double baseline;
    public boolean surge;
    public long emittedAt;

    public WindowResult() {}
}
