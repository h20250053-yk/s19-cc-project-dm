package com.s19.analytics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/**
 * Situation Analytics - real-time stream processing (requirement e).
 *
 * <pre>
 *  Kafka: incident-events, dispatch-events, consistency-events
 *     -> parse JSON envelope                              (EventParser, counts parse errors)
 *     -> event-time watermarks, 10 s out-of-orderness, idle-source handling
 *     -> keyBy(region)
 *     -> 1-minute TUMBLING event-time window              (stateful: counts + HashSet of unique reporters)
 *     -> keyBy(region) -> SurgeDetector                   (stateful across windows: ValueState EWMA baseline)
 *     -> JSON -> Kafka: incident-aggregates               (separate results topic)
 * </pre>
 * Arguments (all optional): --bootstrap 10.10.0.10:9092 --group situation-analytics-flink
 *   --input-topics incident-events,dispatch-events,consistency-events --output-topic incident-aggregates
 *   --window-seconds 60 --checkpoint-dir hdfs:///flink/checkpoints/situation-analytics
 *   --surge-min 5 --surge-factor 2.0
 */
public class SituationAnalyticsJob {

    public static void main(String[] args) throws Exception {
        ParameterTool p = ParameterTool.fromArgs(args);
        String bootstrap = p.get("bootstrap", "10.10.0.10:9092");
        String group = p.get("group", "situation-analytics-flink");
        List<String> topics = Arrays.asList(
                p.get("input-topics", "incident-events,dispatch-events,consistency-events").split(","));
        String outputTopic = p.get("output-topic", "incident-aggregates");
        long windowSeconds = p.getLong("window-seconds", 60L);
        String checkpointDir = p.get("checkpoint-dir", "hdfs:///flink/checkpoints/situation-analytics");
        long surgeMin = p.getLong("surge-min", 5L);
        double surgeFactor = p.getDouble("surge-factor", 2.0);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(p);
        env.setParallelism(p.getInt("parallelism", 1));
        env.enableCheckpointing(30_000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(checkpointDir);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000L);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topics)
                .setGroupId(group)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "60000")
                .build();

        WatermarkStrategy<Event> watermarks = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, recordTs) -> event.ts > 0 ? event.ts : recordTs)
                .withIdleness(Duration.ofSeconds(30));

        DataStream<Event> events = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-disaster-events")
                .flatMap(new EventParser()).name("parse-json-envelope")
                .assignTimestampsAndWatermarks(watermarks).name("event-time-watermarks");

        DataStream<WindowResult> windows = events
                .keyBy(e -> e.region)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSeconds)))
                .aggregate(new WindowAgg(), new WindowToResult())
                .name("tumbling-window-per-region");

        DataStream<String> results = windows
                .keyBy(r -> r.region)
                .process(new SurgeDetector(surgeMin, surgeFactor)).name("surge-detector-keyed-state")
                .map(new ToJson()).name("to-json");

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        results.sinkTo(sink).name("kafka-" + outputTopic);

        env.execute("s19-situation-analytics");
    }

    /** JSON string -> Event. Malformed records are counted and skipped (never fail the job). */
    public static class EventParser extends RichFlatMapFunction<String, Event> {
        private transient ObjectMapper mapper;
        private transient Counter parsed;
        private transient Counter errors;

        @Override
        public void open(Configuration parameters) {
            mapper = new ObjectMapper();
            parsed = getRuntimeContext().getMetricGroup().counter("s19EventsParsed");
            errors = getRuntimeContext().getMetricGroup().counter("s19ParseErrors");
        }

        @Override
        public void flatMap(String value, Collector<Event> out) {
            try {
                JsonNode n = mapper.readTree(value);
                Event e = new Event();
                e.eventType = n.path("eventType").asText("");
                e.region = n.path("region").asText("global");
                if (e.region.isEmpty()) {
                    e.region = "global";
                }
                e.severity = n.path("severity").asText("");
                e.reporterId = n.path("reporterId").asText("");
                e.incidentId = n.path("incidentId").asText("");
                e.resourceCount = n.path("resourceCount").asInt(0);
                e.volunteerCount = n.path("volunteerCount").asInt(0);
                e.repaired = n.path("repaired").asInt(0);
                e.ts = n.path("ts").asLong(0L);
                if (e.eventType.isEmpty()) {
                    errors.inc();
                    return;
                }
                parsed.inc();
                out.collect(e);
            } catch (Exception ex) {
                errors.inc();
            }
        }
    }

    /** Incremental aggregation: only the accumulator is kept in window state, not the raw events. */
    public static class WindowAgg implements AggregateFunction<Event, WindowAccumulator, WindowAccumulator> {
        @Override
        public WindowAccumulator createAccumulator() {
            return new WindowAccumulator();
        }

        @Override
        public WindowAccumulator add(Event value, WindowAccumulator acc) {
            acc.add(value);
            return acc;
        }

        @Override
        public WindowAccumulator getResult(WindowAccumulator acc) {
            return acc;
        }

        @Override
        public WindowAccumulator merge(WindowAccumulator a, WindowAccumulator b) {
            return a.merge(b);
        }
    }

    /** Adds the key and window bounds to the aggregate. */
    public static class WindowToResult
            extends ProcessWindowFunction<WindowAccumulator, WindowResult, String, TimeWindow> {
        @Override
        public void process(String region, Context ctx, Iterable<WindowAccumulator> accs,
                            Collector<WindowResult> out) {
            WindowAccumulator a = accs.iterator().next();
            WindowResult r = new WindowResult();
            r.region = region;
            r.windowStart = ctx.window().getStart();
            r.windowEnd = ctx.window().getEnd();
            r.incidents = a.incidents;
            r.critical = a.critical;
            r.updates = a.updates;
            r.uniqueReporters = a.reporters.size();
            r.dispatches = a.dispatches;
            r.resourcesDispatched = a.resourcesDispatched;
            r.volunteersDispatched = a.volunteersDispatched;
            r.resolved = a.resolved;
            r.conflicts = a.conflicts;
            r.readRepairs = a.readRepairs;
            r.quorumFailures = a.quorumFailures;
            r.antiEntropyRepairs = a.antiEntropyRepairs;
            out.collect(r);
        }
    }

    /**
     * Keyed state across windows: exponentially weighted baseline of incidents per window.
     * A window is a SURGE when incidents >= surgeMin and incidents >= surgeFactor x baseline.
     */
    public static class SurgeDetector extends KeyedProcessFunction<String, WindowResult, WindowResult> {
        private final long surgeMin;
        private final double surgeFactor;
        private transient ValueState<Double> baseline;
        private transient ValueState<Long> previous;
        private transient Counter surges;

        public SurgeDetector(long surgeMin, double surgeFactor) {
            this.surgeMin = surgeMin;
            this.surgeFactor = surgeFactor;
        }

        @Override
        public void open(Configuration parameters) {
            baseline = getRuntimeContext().getState(new ValueStateDescriptor<>("ewma-baseline", Types.DOUBLE));
            previous = getRuntimeContext().getState(new ValueStateDescriptor<>("previous-incidents", Types.LONG));
            surges = getRuntimeContext().getMetricGroup().counter("s19SurgesDetected");
        }

        @Override
        public void processElement(WindowResult r, Context ctx, Collector<WindowResult> out) throws Exception {
            Double base = baseline.value();
            Long prev = previous.value();
            double b = base == null ? 0.0 : base;
            r.prevIncidents = prev == null ? 0L : prev;
            r.baseline = Math.round(b * 100.0) / 100.0;
            r.surge = r.incidents >= surgeMin && (base == null || r.incidents >= surgeFactor * Math.max(b, 1.0));
            if (r.surge) {
                surges.inc();
            }
            baseline.update(base == null ? (double) r.incidents : 0.7 * b + 0.3 * r.incidents);
            previous.update(r.incidents);
            r.emittedAt = System.currentTimeMillis();
            out.collect(r);
        }
    }

    public static class ToJson extends RichMapFunction<WindowResult, String> {
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) {
            mapper = new ObjectMapper();
        }

        @Override
        public String map(WindowResult r) throws Exception {
            return mapper.writeValueAsString(r);
        }
    }
}
