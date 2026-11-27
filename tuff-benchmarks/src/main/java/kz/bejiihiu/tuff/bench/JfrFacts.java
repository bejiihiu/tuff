package kz.bejiihiu.tuff.bench;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedThread;
import jdk.jfr.consumer.RecordingFile;

/**
 * Reads the JFR recording the server wrote, without any external tool.
 *
 * <p>The recording is the only source of CPU, allocation and thread churn facts that does not depend on
 * the server's own counters, and it is also the only source that is identical on EDF and Kairos. Values
 * are restricted to the measurement window so the warm-up cannot flatter a run.
 *
 * <p>The parser never fails a run: a missing or unreadable recording leaves every fact at {@code NaN}
 * and sets {@link Facts#present()} to {@code false}, and the report says so.
 */
final class JfrFacts {

    private JfrFacts() {
    }

    /**
     * Extracts the window's facts from a recording.
     *
     * @param recording recording file
     * @param fromEpoch start of the window, inclusive
     * @param toEpoch   end of the window, exclusive
     * @return the facts, never {@code null}
     */
    static Facts read(final Path recording, final long fromEpoch, final long toEpoch) {
        if (!Files.isRegularFile(recording)) {
            return Facts.absent("recording missing");
        }
        double cpuUser = 0.0d;
        double cpuSystem = 0.0d;
        int cpuSamples = 0;
        long gcCount = 0L;
        long gcPauseNanos = 0L;
        final java.util.List<Long> gcPauses = new java.util.ArrayList<>();
        long heapMaxBytes = -1L;
        long threadStarts = 0L;
        long threadEnds = 0L;
        final Map<String, Integer> threadSamples = new LinkedHashMap<>();
        try (RecordingFile file = new RecordingFile(recording)) {
            while (file.hasMoreEvents()) {
                final RecordedEvent event = file.readEvent();
                final Instant start = event.getStartTime();
                if (start == null) {
                    continue;
                }
                final long epoch = start.toEpochMilli();
                if (epoch < fromEpoch || epoch >= toEpoch) {
                    continue;
                }
                switch (event.getEventType().getName()) {
                    case "jdk.CPULoad" -> {
                        if (event.hasField("jvmUser") && event.hasField("jvmSystem")) {
                            cpuUser += event.getFloat("jvmUser");
                            cpuSystem += event.getFloat("jvmSystem");
                            ++cpuSamples;
                        }
                    }
                    case "jdk.GCPhasePause" -> {
                        final long nanos = event.getDuration() == null ? 0L : event.getDuration().toNanos();
                        ++gcCount;
                        gcPauseNanos += nanos;
                        gcPauses.add(nanos);
                    }
                    case "jdk.GCHeapSummary" -> {
                        if (event.hasField("heapUsed")) {
                            heapMaxBytes = Math.max(heapMaxBytes, event.getLong("heapUsed"));
                        }
                    }
                    case "jdk.ThreadStart" -> ++threadStarts;
                    case "jdk.ThreadEnd" -> ++threadEnds;
                    case "jdk.ExecutionSample" -> {
                        if (event.hasField("sampledThread")) {
                            final RecordedThread thread = event.getThread("sampledThread");
                            final String name = thread == null ? "unknown" : thread.getJavaName();
                            threadSamples.merge(name == null ? "unknown" : name, 1, Integer::sum);
                        }
                    }
                    default -> {
                        // every other event is outside this report's scope
                    }
                }
            }
        } catch (final IOException | RuntimeException failure) {
            return Facts.absent("recording unreadable: " + failure.getMessage());
        }
        gcPauses.sort(Long::compare);
        final Map<String, Integer> top = threadSamples.entrySet().stream()
            .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
            .limit(10)
            .collect(java.util.stream.Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (left, right) -> left,
                LinkedHashMap::new
            ));
        return new Facts(
            true,
            "",
            cpuSamples == 0 ? Double.NaN : cpuUser / cpuSamples,
            cpuSamples == 0 ? Double.NaN : cpuSystem / cpuSamples,
            cpuSamples,
            gcCount,
            gcPauseNanos / 1_000_000.0d,
            gcPauses.isEmpty() ? Double.NaN : gcPauses.get(gcPauses.size() - 1) / 1_000_000.0d,
            gcPauses.isEmpty() ? Double.NaN : Stats.percentile(gcPauses.stream().mapToLong(Long::longValue).toArray(), 0.99)
                / 1_000_000.0d,
            heapMaxBytes < 0L ? Double.NaN : heapMaxBytes / (1024.0d * 1024.0d),
            threadStarts,
            threadEnds,
            Map.copyOf(top)
        );
    }

    /**
     * CPU, GC and thread facts of one measurement window.
     *
     * @param present        whether the recording could be read
     * @param reason         why not, or the empty string
     * @param cpuUser        mean JVM user CPU as a fraction of one machine
     * @param cpuSystem      mean JVM system CPU as a fraction of one machine
     * @param cpuSamples     number of {@code jdk.CPULoad} events in the window
     * @param gcCount        number of pause phases
     * @param gcPauseMillis  total pause time
     * @param gcPauseMaxMillis longest single pause
     * @param gcPauseP99Millis 99th percentile pause
     * @param heapUsedMaxMb  highest used heap in the window
     * @param threadStarts   threads started in the window
     * @param threadEnds     threads ended in the window
     * @param topThreads     execution sample counts by thread name, top ten
     */
    record Facts(
        boolean present,
        String reason,
        double cpuUser,
        double cpuSystem,
        int cpuSamples,
        long gcCount,
        double gcPauseMillis,
        double gcPauseMaxMillis,
        double gcPauseP99Millis,
        double heapUsedMaxMb,
        long threadStarts,
        long threadEnds,
        Map<String, Integer> topThreads
    ) {

        /**
         * Returns the value of an unreadable recording.
         *
         * @param reason why the facts are missing
         * @return an absent fact set
         */
        static Facts absent(final String reason) {
            return new Facts(
                false,
                reason,
                Double.NaN,
                Double.NaN,
                0,
                -1L,
                Double.NaN,
                Double.NaN,
                Double.NaN,
                Double.NaN,
                -1L,
                -1L,
                Map.of()
            );
        }
    }
}
