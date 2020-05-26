import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

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

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: JavaStructuredSessionization <hostname> <port>");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStructuredSessionization")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to host:port
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", host)
                .option("port", port)
                .option("includeTimestamp", true)
                .load();
        /*
         * [YO]
         * current dataset schema is:
         *  value: String
         *  timestamp: Timestamp
         */

        FlatMapFunction<LineWithTimestamp, WordEvent> linesToEvents = new LineToWordEvents();

        // Split the lines into words, treat words as sessionId of events
        Dataset<WordEvent> events = lines
                .withColumnRenamed("value", "line")
                // cast Un-typed dataset (Dataset<Row>) to strictly typed dataset of defined POJO (Dataset<WordEvent>)
                .as(Encoders.bean(LineWithTimestamp.class))
                .flatMap(linesToEvents, Encoders.bean(WordEvent.class));

        // Sessionize the events. Track number of events, start and end timestamps of session, and
        // and report session updates.
        //
        // Step 1: Define the state update function

        final long timeoutMs = 10000L; // session timeout is 10 secs
        FlatMapGroupsWithStateFunction<String, WordEvent, SessionInfo, SessionUpdate> stateUpdateFunc = new Sessionize(timeoutMs);

        // Step 2: Apply the state update function to the events streaming Dataset grouped by sessionId
        final Encoder<SessionInfo> stateEncoder = Encoders.bean(SessionInfo.class);
        final Encoder<SessionUpdate> returnValueEncoder = Encoders.bean(SessionUpdate.class);
        final GroupStateTimeout timeoutConf = GroupStateTimeout.ProcessingTimeTimeout();
        Dataset<SessionUpdate> sessionUpdates = events.groupByKey(new GroupByImpl(), Encoders.STRING())
                .flatMapGroupsWithState(
                        stateUpdateFunc,
                        OutputMode.Update(),
                        stateEncoder,
                        returnValueEncoder,
                        timeoutConf);

        // Start running the query that prints the session updates to the console
        StreamingQuery query = sessionUpdates
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();

        query.awaitTermination();
    }

    /**
     * [YO]
     * Map (convert) string line to list of WordEvents
     */
    public static class LineToWordEvents implements FlatMapFunction<LineWithTimestamp, WordEvent> {

        @Override
        public Iterator<WordEvent> call(LineWithTimestamp lineWithTimestamp) {
            ArrayList<WordEvent> eventList = new ArrayList<>();
            for (String word : lineWithTimestamp.getLine().split(" ")) {
                eventList.add(new WordEvent(word, lineWithTimestamp.getTimestamp()));
            }
            return eventList.iterator();
        }
    }


    /**
     * [YO]
     * simple group by implementation: event is a word
     */
    public static class GroupByImpl implements MapFunction<WordEvent, String> {
        @Override
        public String call(WordEvent wordEvent) {
            return wordEvent.getWord();
        }
    }

    /**
     * [YO]
     * Simple sesionzation business logic implementation: sessionize words
     */
    public static class Sessionize implements FlatMapGroupsWithStateFunction<String, WordEvent, SessionInfo, SessionUpdate> {

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
        public Iterator<SessionUpdate> call(String key, Iterator<WordEvent> wordEvents, GroupState<SessionInfo> state) {
            final List<SessionUpdate> sessionUpdates =  new ArrayList<>();
            // If timed out, then remove session and send final update
            if (state.hasTimedOut()) {
                final SessionInfo oldSession = state.get();
                final long durationMs = oldSession.calculateDuration();
                final int numEvents = oldSession.getNumEvents();
                final String sessionId = oldSession.sessionId;
                SessionUpdate finalUpdate = new SessionUpdate(sessionId, durationMs, numEvents, true);
                state.remove();
                sessionUpdates.add(finalUpdate);
                return sessionUpdates.iterator();

            }
            List<WordEvent> events =  new ArrayList<>();
            wordEvents.forEachRemaining(events::add);
            events = events.stream().sorted(Comparator.comparingLong(o -> o.timestamp.getTime())).collect(Collectors.toList());
            SessionInfo currentSession = null;
            if (state.exists()) {
                currentSession = state.get();
            }

            for (WordEvent event : events) {
                final long eventTimeMs = event.timestamp.getTime();
                // current session could be null IFF state.exists is False and we are on first event
                if(currentSession != null) {
                    final long timeDiffMs = currentSession.endTimestampMs - eventTimeMs;
                    // TODO[yo]: handle late events - events that starts before current session startTimestampMs
                    if (timeDiffMs <= sessionTimeoutMs) {
                        currentSession.numEvents++;
                        currentSession.startTimestampMs = Math.max(currentSession.startTimestampMs, eventTimeMs);
                        currentSession.endTimestampMs = Math.max(currentSession.endTimestampMs, eventTimeMs);
                        continue;
                    }
                    // session timeout
                    final String sessionId = currentSession.sessionId;
                    final long durationMs = currentSession.calculateDuration();
                    final int numEvents = currentSession.getNumEvents();

                    SessionUpdate sessionUpdate = new SessionUpdate(sessionId, durationMs, numEvents, true);
                    sessionUpdates.add(sessionUpdate);
                }
                currentSession = new SessionInfo();
                currentSession.sessionId = UUID.randomUUID().toString();
                currentSession.numEvents++;
                currentSession.startTimestampMs = eventTimeMs;
                currentSession.endTimestampMs = eventTimeMs;
            }
            state.update(currentSession);
            state.setTimeoutDuration(sessionTimeoutMs);

            return sessionUpdates.iterator();
        }
    }

    /**
     * User-defined data type representing the raw lines with timestamps.
     */
    @NoArgsConstructor // [YO] required for de-serializing POJO object in Spark
    @Getter
    @Setter
    public static class LineWithTimestamp implements Serializable {
        private String line;
        private Timestamp timestamp;
    }

    /**
     * User-defined data type representing the input events
     */
    @NoArgsConstructor // [YO] required for de-serializing POJO object in Spark
    @Getter
    @Setter
    public static class WordEvent implements Serializable {
        private String word;
        private Timestamp timestamp;

        public WordEvent(String word, Timestamp timestamp) {
            this.word = word;
            this.timestamp = timestamp;
        }
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

        @Override public String toString() {
            return "SessionInfo(numEvents = " + numEvents +
                    ", timestamps = " + startTimestampMs + " to " + endTimestampMs + ")";
        }
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