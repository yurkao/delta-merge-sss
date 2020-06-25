import io.delta.tables.DeltaTable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network.
 * <p>
 * Usage: JavaStructuredNetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Structured Streaming
 * would connect to receive data.
 * <p>
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9999`
 * and then run the example
 * `$ bin/run-example sql.streaming.JavaStructuredSessionization
 * localhost 9999`
 */
@Slf4j
public final class JavaStructuredSessionization {
    public static void main(String[] args) throws Exception {
        final Encoder<RawEvent> rawEventsEncoder = Encoders.bean(RawEvent.class);
        final StructType inputSchema = rawEventsEncoder.schema();
        log.warn("Input schema: {}", inputSchema.prettyJson());
        if (args.length < 2) {
            System.err.println("Usage (foreachBatch + append to delta): JavaStructuredSessionization <in-json-path> <out-delta-path>");
            System.err.println("Usage (foreachBatch + merge to delta - no persist): JavaStructuredSessionization <in-json-path> <out-delta-path> merge");
            System.err.println("Usage (foreachBatch + append to delta WITH persist): JavaStructuredSessionization <in-json-path> <out-delta-path> merge+persist");
            System.exit(1);
        }

        final String inPath = args[0];
        final String outPath = args[1];

        final SparkSession spark = SparkSession.builder().getOrCreate();


        final Dataset<Row> df = spark
                .readStream()
                .schema(inputSchema)
                .format("json")
                .option("path", inPath)
                .option("maxFilesPerTrigger", "1")
                .option("latestFirst", "false")
                .load();

        final Column recordCol = functions.struct("ip", "fqdn", "tenant_id");
        final Dataset<Event> events = df
                .withColumn("watermark", functions.col("event_time_utc_ts").divide(1000).cast("timestamp"))
                .withWatermark("watermark", "1 minute")
                .withColumn("record", recordCol)
                .withColumnRenamed("event_time_utc_ts", "eventTime")
                .as(Encoders.bean(Event.class));

        final long timeoutMs = 5*60*1000L; // 5 minutes for timeout
        FlatMapGroupsWithStateFunction<Record, Event, SessionInfo, SessionUpdate> stateUpdateFunc = new Sessionize(timeoutMs);

        final Encoder<SessionInfo> stateEncoder = Encoders.bean(SessionInfo.class);
        final Encoder<SessionUpdate> returnValueEncoder = Encoders.bean(SessionUpdate.class);
        final GroupStateTimeout timeoutConf = GroupStateTimeout.ProcessingTimeTimeout();
        Dataset<SessionUpdate> sessionUpdates = events.groupByKey(new GroupByImpl(), Encoders.bean(Record.class))
                .flatMapGroupsWithState(
                        stateUpdateFunc,
                        OutputMode.Update(),
                        stateEncoder,
                        returnValueEncoder,
                        timeoutConf);

        final VoidFunction2<Dataset<Row>, Long>  sink;
        if (args.length==3) {

            if ("merge+persist".equals(args[2])) {
                sink = new DeltaMergePersistSink(spark, outPath);
            } else {
                sink = new DeltaMergeSink(spark, outPath);
            }
        } else {
            sink = new DeltaAppendSink(outPath);
        }
        log.warn("Using {} sink", sink);

        final Trigger trigger = Trigger.ProcessingTime(0);
        log.warn("Using trigger: {}", trigger);
        final StreamingQuery query = sessionUpdates.toDF()
                .writeStream()
                .option("checkpointLocation", outPath + ".checkpoint")
                .outputMode(OutputMode.Update())
                .queryName("job")
                .trigger(trigger)
                .foreachBatch(sink)
                .start();

        query.awaitTermination();
    }

    @Slf4j
    static class DeltaAppendSink implements VoidFunction2<Dataset<Row>, Long> {
        private final String outPath;

        DeltaAppendSink(String outPath) {
            log.info("Saving output to {}", outPath);

            this.outPath = outPath;
        }

        @Override
        public void call(Dataset<Row> batchDf, Long v2) {
            batchDf.write().format("delta").mode(SaveMode.Append).save(outPath);
        }

        @Override
        public String toString() {
            return "foreachBatch delta append";
        }

    }

    @Slf4j
    static class DeltaMergeSink implements VoidFunction2<Dataset<Row>, Long> {
        final DeltaTable table;
        final Column upsertMatch;

        DeltaMergeSink(SparkSession spark, String outPath) throws IOException {
            final Configuration fsConf = spark.sparkContext().hadoopConfiguration();
            final FileSystem fileSystem = FileSystem.get(fsConf);
            final boolean exists = fileSystem.exists(new Path(outPath));
            if (!exists) {
                final StructType schema = Encoders.bean(SessionUpdate.class).schema();
                log.warn("Creating DeltaTable {} with output schema {}", outPath, schema.prettyJson());
                final Dataset<Row> emptyDf = spark.createDataFrame(Collections.emptyList(), schema);
                emptyDf.write().format("delta").save(outPath);
            }
            table = DeltaTable.forPath(spark, outPath);
            upsertMatch = functions.expr("sessions.id = updates.id");

        }

        @Override
        public void call(Dataset<Row> batchDf, Long batchId) {
            table.as("sessions").merge(batchDf.as("updates"), upsertMatch)
                    .whenNotMatched().insertAll()           // new session to be added
                    .whenMatched()
                    .updateAll()
                    .execute();
        }

        @Override
        public String toString() {
            return "foreachBatch delta merge (no persist)";
        }
    }

    static class DeltaMergePersistSink extends DeltaMergeSink {
        DeltaMergePersistSink(SparkSession spark, String outPath) throws IOException {
            super(spark, outPath);
        }

        @Override
        public void call(Dataset<Row> batchDf, Long batchId) {
            final Dataset<Row> persistDf = batchDf.persist();
            super.call(persistDf, batchId);
            persistDf.unpersist();
        }

        @Override
        public String toString() {
            return "foreachBatch delta merge+persist";
        }
    }

    /**
     * [YO]
     * simple group by implementation: event is a word
     */
    public static class GroupByImpl implements MapFunction<Event, Record> {
        @Override
        public Record call(Event event) {
            return event.getRecord();
        }
    }

    /**
     * [YO]
     * Simple sesionzation business logic implementation: sessionize words
     */
    public static class Sessionize implements FlatMapGroupsWithStateFunction<Record, Event, SessionInfo, SessionUpdate> {

        private final long sessionTimeoutMs;

        public Sessionize(long sessionTimeoutMs) {
            this.sessionTimeoutMs = sessionTimeoutMs;
        }

        /**
         *
         * @param key the return value of GroupByImpl.call
         * @param wordEvents list of wordsEvents matching the @key
         * @param state session state
         * @return created/updated/expired sessions
         */
        @Override
        public Iterator<SessionUpdate> call(Record key, Iterator<Event> wordEvents, GroupState<SessionInfo> state) {
            final List<SessionUpdate> sessionUpdates =  new ArrayList<>();
            // If timed out, then remove session and send final update
            if (state.hasTimedOut()) {
                final SessionInfo oldSession = state.get();
                final long durationMs = oldSession.calculateDuration();
                final int numEvents = oldSession.getNumEvents();
                final String sessionId = oldSession.sessionId;
                final SessionUpdate finalUpdate = new SessionUpdate(sessionId, durationMs, numEvents, true);
                state.remove();
                sessionUpdates.add(finalUpdate);
                return sessionUpdates.iterator();

            }
            final List<Event> events =  new ArrayList<>();
            wordEvents.forEachRemaining(events::add);
            events.sort(Comparator.comparingLong(e -> e.eventTime));
            SessionInfo currentSession;
            final boolean created;

            if (state.exists()) {
                currentSession = state.get();
                created = false;
            } else {
                created = true;
                currentSession = new SessionInfo();
                currentSession.sessionId = UUID.randomUUID().toString();
                currentSession.startTimestampMs = Long.MAX_VALUE;
                currentSession.endTimestampMs = Long.MIN_VALUE;
            }

            for (Event event : events) {
                final long eventTimeMs = event.eventTime;
                currentSession.startTimestampMs = Math.min(eventTimeMs, currentSession.startTimestampMs);
                currentSession.numEvents++;
                currentSession.endTimestampMs = Math.max(eventTimeMs, currentSession.startTimestampMs);
            }

            if (!created) {
                // new session object should be created - otherwise the spark state will not be updated
                // with old POJO and will be timed-out
                final SessionInfo updatedSession = new SessionInfo();
                updatedSession.numEvents = currentSession.numEvents;
                updatedSession.sessionId = currentSession.sessionId;
                updatedSession.startTimestampMs = currentSession.startTimestampMs;
                updatedSession.endTimestampMs = currentSession.endTimestampMs;
                currentSession = updatedSession;
            }

            state.update(currentSession);
            state.setTimeoutDuration(sessionTimeoutMs);
            final String sessionId = currentSession.sessionId;
            final long durationMs = currentSession.calculateDuration();
            final int numEvents = currentSession.getNumEvents();
            final SessionUpdate sessionUpdate = new SessionUpdate(sessionId, durationMs, numEvents, false);
            sessionUpdates.add(sessionUpdate);
            return sessionUpdates.iterator();
        }
    }

    @Getter
    @Setter
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    static class RawEvent {
        private long event_time_utc_ts;
        private String tenant_id;
        private String ip;
        private String fqdn;
    }


    @NoArgsConstructor // default constructor (bean) for Spark: row to java object conversion.
    @EqualsAndHashCode
    @Getter
    @Setter
    public static class Record implements Serializable {

        private String tenant_id;
        private String ip;
        private String fqdn;
    }
    /**
     * User-defined data type representing the input events
     */
    @NoArgsConstructor // [YO] required for de-serializing POJO object in Spark
    @Getter
    @Setter
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    public static class Event implements Serializable {
        @EqualsAndHashCode.Include
        private Record record;

        @EqualsAndHashCode.Include
        private long eventTime;

    }

    /**
     * User-defined data type for storing a session information as state in mapGroupsWithState.
     */
    @NoArgsConstructor // [YO] required for de-serializing POJO object in Spark
    @Getter
    @Setter
    public static class SessionInfo implements Serializable {
        String sessionId;
        private int numEvents = 0;
        private long startTimestampMs = -1;
        private long endTimestampMs = -1;

        public long calculateDuration() { return endTimestampMs - startTimestampMs; }

    }

    /**
     * User-defined data type representing the update information returned by mapGroupsWithState.
     */
    @NoArgsConstructor // [YO] required for de-serializing POJO object in Spark
    @Getter
    @Setter
    public static class SessionUpdate implements Serializable {
        private String id;
        private long durationMs;
        private int numEvents;
        private boolean expired;

        public SessionUpdate(String id, long durationMs, int numEvents, boolean expired) {
            this.id = id;
            this.durationMs = durationMs;
            this.numEvents = numEvents;
            this.expired = expired;
        }
    }
}