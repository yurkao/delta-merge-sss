import io.delta.tables.DeltaTable;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Timestamp;
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
public final class JavaStructuredSessionization {
    private static final Logger LOG = LoggerFactory.getLogger("JavaStructuredSessionization");

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: JavaStructuredSessionization <hostname> <port> <delta>");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String deltaPath = args[2];

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStructuredSessionization")
                .getOrCreate();
        final StructType schema = Encoders.bean(SessionUpdate.class).schema();
        LOG.warn("Creating delta at {} with schema {}", deltaPath, schema);
        spark.createDataFrame(Collections.emptyList(), schema).write()
                .mode(SaveMode.Overwrite)
                .format("delta")
                .save(deltaPath);
        LOG.warn("Loading delta from {}", deltaPath);
        final DeltaTable deltaTable = DeltaTable.forPath(deltaPath);
        LOG.warn("Loading delta from {} : done", deltaPath);

        final Column mergeExpr = functions.expr("sessions.id = updates.id");
        LOG.warn("Using delta merge expression {}", mergeExpr);
        // Create DataFrame representing the stream of input lines from connection to host:port
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", host)
                .option("port", port)
                .option("includeTimestamp", true)
                .load();

        FlatMapFunction<LineWithTimestamp, Event> linesToEvents =
                new FlatMapFunction<LineWithTimestamp, Event>() {
                    @Override
                    public Iterator<Event> call(LineWithTimestamp lineWithTimestamp) {
                        ArrayList<Event> eventList = new ArrayList<>();
                        for (String word : lineWithTimestamp.getLine().split(" ")) {
                            eventList.add(new Event(word, lineWithTimestamp.getTimestamp()));
                        }
                        return eventList.iterator();
                    }
                };

        // Split the lines into words, treat words as sessionId of events
        Dataset<Event> events = lines
                .withColumnRenamed("value", "line")
                .as(Encoders.bean(LineWithTimestamp.class))
                .flatMap(linesToEvents, Encoders.bean(Event.class))
                .withWatermark("timestamp", "10 second");

        // Sessionize the events. Track number of events, start and end timestamps of session, and
        // and report session updates.
        //
        // Step 1: Define the state update function
        MapGroupsWithStateFunction<String, Event, SessionInfo, SessionUpdate> stateUpdateFunc =
                new MyMapGroupsWithStateFunction();

        // Step 2: Apply the state update function to the events streaming Dataset grouped by sessionId
        Dataset<SessionUpdate> sessionUpdates = events
                .groupByKey(
                        new MapFunction<Event, String>() {
                            @Override public String call(Event event) {
                                return event.getSessionId();
                            }
                        }, Encoders.STRING())
                .mapGroupsWithState(
                        stateUpdateFunc,
                        Encoders.bean(SessionInfo.class),
                        Encoders.bean(SessionUpdate.class),
                        GroupStateTimeout.ProcessingTimeTimeout());

        // Start running the query that prints the session updates to the console
        StreamingQuery query = sessionUpdates
                .writeStream()
                .outputMode("update")
                .foreachBatch((VoidFunction2<Dataset<SessionUpdate>, Long>) (batchDf, v2) -> {
                    // following doubles number of spark state rows and causes MapGroupsWithStateFunction to log twice
                    deltaTable.as("sessions").merge(batchDf.toDF().as("updates"), mergeExpr)
                            .whenNotMatched().insertAll()
                            .whenMatched()
                            .updateAll()
                            .execute();
                })
                .trigger(Trigger.ProcessingTime(10000))
                .queryName("ACME")
                .start();

        query.awaitTermination();
    }
    public static class MyMapGroupsWithStateFunction implements MapGroupsWithStateFunction<String, Event, SessionInfo, SessionUpdate> {
        private static final Logger LOG = LoggerFactory.getLogger("ACME");
        @Override
        public SessionUpdate call(String sessionId, Iterator<Event> events, GroupState<SessionInfo> state) throws Exception  {
            // If timed out, then remove session and send final update
            if (state.hasTimedOut()) {
                SessionUpdate finalUpdate = new SessionUpdate(
                        sessionId, state.get().calculateDuration(), state.get().getNumEvents(), true);
                state.remove();
                return finalUpdate;

            } else {
                // Find max and min timestamps in events
                long maxTimestampMs = Long.MIN_VALUE;
                long minTimestampMs = Long.MAX_VALUE;
                int numNewEvents = 0;
                while (events.hasNext()) {
                    Event e = events.next();
                    long timestampMs = e.getTimestamp().getTime();
                    maxTimestampMs = Math.max(timestampMs, maxTimestampMs);
                    minTimestampMs = Math.min(timestampMs, minTimestampMs);
                    numNewEvents += 1;
                }
                SessionInfo updatedSession = new SessionInfo();

                // Update start and end timestamps in session
                if (state.exists()) {
                    SessionInfo oldSession = state.get();
                    updatedSession.setNumEvents(oldSession.numEvents + numNewEvents);
                    updatedSession.setStartTimestampMs(oldSession.startTimestampMs);
                    updatedSession.setEndTimestampMs(Math.max(oldSession.endTimestampMs, maxTimestampMs));
                } else {
                    updatedSession.setNumEvents(numNewEvents);
                    updatedSession.setStartTimestampMs(minTimestampMs);
                    updatedSession.setEndTimestampMs(maxTimestampMs);
                    LOG.warn("New session will be added to state: {}", updatedSession);
                }
                state.update(updatedSession);
                // Set timeout such that the session will be expired if no data received for 10 seconds
                state.setTimeoutDuration("180 seconds");
                return new SessionUpdate(
                        sessionId, state.get().calculateDuration(), state.get().getNumEvents(), false);
            }
        }
    }
    /**
     * User-defined data type representing the raw lines with timestamps.
     */
    public static class LineWithTimestamp implements Serializable {
        private String line;
        private Timestamp timestamp;

        public Timestamp getTimestamp() { return timestamp; }
        public void setTimestamp(Timestamp timestamp) { this.timestamp = timestamp; }

        public String getLine() { return line; }
        public void setLine(String sessionId) { this.line = sessionId; }
    }

    /**
     * User-defined data type representing the input events
     */
    public static class Event implements Serializable {
        private String sessionId;
        private Timestamp timestamp;

        public Event() { }
        public Event(String sessionId, Timestamp timestamp) {
            this.sessionId = sessionId;
            this.timestamp = timestamp;
        }

        public Timestamp getTimestamp() { return timestamp; }
        public void setTimestamp(Timestamp timestamp) { this.timestamp = timestamp; }

        public String getSessionId() { return sessionId; }
        public void setSessionId(String sessionId) { this.sessionId = sessionId; }
    }

    /**
     * User-defined data type for storing a session information as state in mapGroupsWithState.
     */
    public static class SessionInfo implements Serializable {
        private int numEvents = 0;
        private long startTimestampMs = -1;
        private long endTimestampMs = -1;

        public int getNumEvents() { return numEvents; }
        public void setNumEvents(int numEvents) { this.numEvents = numEvents; }

        public long getStartTimestampMs() { return startTimestampMs; }
        public void setStartTimestampMs(long startTimestampMs) {
            this.startTimestampMs = startTimestampMs;
        }

        public long getEndTimestampMs() { return endTimestampMs; }
        public void setEndTimestampMs(long endTimestampMs) { this.endTimestampMs = endTimestampMs; }

        public long calculateDuration() { return endTimestampMs - startTimestampMs; }

        @Override public String toString() {
            return "SessionInfo(numEvents = " + numEvents +
                    ", timestamps = " + startTimestampMs + " to " + endTimestampMs + ")";
        }
    }

    /**
     * User-defined data type representing the update information returned by mapGroupsWithState.
     */
    public static class SessionUpdate implements Serializable {
        private String id;
        private long durationMs;
        private int numEvents;
        private boolean expired;

        public SessionUpdate() { }

        public SessionUpdate(String id, long durationMs, int numEvents, boolean expired) {
            this.id = id;
            this.durationMs = durationMs;
            this.numEvents = numEvents;
            this.expired = expired;
        }

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }

        public long getDurationMs() { return durationMs; }
        public void setDurationMs(long durationMs) { this.durationMs = durationMs; }

        public int getNumEvents() { return numEvents; }
        public void setNumEvents(int numEvents) { this.numEvents = numEvents; }

        public boolean isExpired() { return expired; }
        public void setExpired(boolean expired) { this.expired = expired; }
    }
}