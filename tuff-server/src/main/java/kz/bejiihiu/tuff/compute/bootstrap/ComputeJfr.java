package kz.bejiihiu.tuff.compute.bootstrap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import jdk.jfr.Configuration;
import jdk.jfr.FlightRecorder;
import jdk.jfr.Recording;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.Nullable;

/**
 * Flight Recorder control for the {@code /tuff compute jfr} subcommand.
 *
 * <p>A recording started here is a separate, named {@link Recording} that uses the JVM's
 * {@code default} configuration and additionally enables the {@code tuff.compute} event category. Age
 * and size are bounded, so leaving it running cannot fill the disk. The recording is a diagnostic
 * tool, not a metrics backend: the scheduler emits its events only when
 * {@code async-compute.metrics.jfr} is enabled, which the command checks before starting anything.
 *
 * <p>Starting, stopping and dumping are handed to one service thread, never run on the thread that
 * invoked the command: a player runs it on a region thread, and a sixty four megabyte dump on a slow
 * disk must not stop that region from ticking. The answer is delivered to the callback on the service
 * thread once the operation finished, and is also logged. Calls are serialised on one lock because
 * only one Tuff recording may exist at a time; repeated calls are safe.
 *
 * <p>The service thread is a daemon and only starts when the first command arrives. {@link #shutdown()}
 * writes a running recording out and stops the service, so no thread survives the server.
 */
final class ComputeJfr {

    /**
     * Name of the recording, visible in {@code jcmd <pid> JFR.check}.
     */
    static final String RECORDING_NAME = "tuff-compute";

    private static final String EVENT_CATEGORY = "tuff.compute";
    private static final Path OUTPUT_DIRECTORY = Path.of("debug");
    private static final Duration MAX_AGE = Duration.ofMinutes(30L);
    private static final long MAX_SIZE_BYTES = 64L * 1024L * 1024L;
    private static final String EXTENSION = ".jfr";
    private static final String SERVICE_THREAD_NAME = "Tuff Compute JFR";
    private static final DateTimeFormatter FILE_TIMESTAMP =
        DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH-mm-ss-SSS'Z'", Locale.ROOT).withZone(ZoneOffset.UTC);
    private static final Logger LOGGER = LogManager.getLogger("Tuff/Compute");

    private static final Object LOCK = new Object();

    private static final ExecutorService SERVICE = Executors.newSingleThreadExecutor(
        Thread.ofPlatform().daemon().name(SERVICE_THREAD_NAME).factory()
    );

    private static @Nullable Recording active;
    private static volatile boolean stopped;

    private ComputeJfr() {
    }

    /**
     * Starts a recording on the service thread.
     *
     * @param callback receives the outcome once the recording is running, on the service thread
     */
    static void start(final Consumer<Result> callback) {
        submit("start", ComputeJfr::startNow, callback);
    }

    /**
     * Stops the recording and writes what it captured, on the service thread.
     *
     * @param callback receives the outcome once the file is on disk, on the service thread
     */
    static void stop(final Consumer<Result> callback) {
        submit("stop", ComputeJfr::stopNow, callback);
    }

    /**
     * Copies the running recording to a file without stopping it, on the service thread.
     *
     * @param callback receives the outcome once the file is on disk, on the service thread
     */
    static void dump(final Consumer<Result> callback) {
        submit("dump", ComputeJfr::dumpNow, callback);
    }

    /**
     * Runs one Flight Recorder operation on the service thread and reports the result.
     *
     * <p>Package private so the off-thread contract can be tested without touching a real recording.
     *
     * @param operation name used in the failure log
     * @param action    operation to run
     * @param callback  receiver of the outcome, called on the service thread
     */
    static void submit(final String operation, final Supplier<Result> action, final Consumer<Result> callback) {
        if (stopped) {
            callback.accept(new Result(false, "The compute JFR service is stopped; the server is shutting down."));
            return;
        }
        try {
            SERVICE.execute(() -> {
                final Result result;
                try {
                    result = action.get();
                } catch (final RuntimeException | Error ex) {
                    LOGGER.error("The JFR {} operation failed", operation, ex);
                    callback.accept(new Result(false, "The JFR " + operation + " operation failed: " + ex.getMessage()));
                    return;
                }
                if (result.success()) {
                    LOGGER.info(result.message());
                } else {
                    LOGGER.warn(result.message());
                }
                try {
                    callback.accept(result);
                } catch (final RuntimeException ex) {
                    LOGGER.warn("Could not deliver the JFR {} result to the command sender", operation, ex);
                }
            });
        } catch (RejectedExecutionException _) {
            callback.accept(new Result(false, "The compute JFR service is stopped; the server is shutting down."));
        }
    }

    /**
     * Stops and writes an active recording during server shutdown, then stops the service thread.
     *
     * <p>Never throws: shutdown must continue even when the recording cannot be written.
     */
    static void shutdown() {
        try {
            synchronized (LOCK) {
                stopped = true;
                final @Nullable Recording recording = active;
                if (recording != null) {
                    active = null;
                    try {
                        recording.stop();
                        LOGGER.info("Stopped the compute JFR recording on shutdown, wrote {}", write(recording).toAbsolutePath());
                    } catch (final IOException | RuntimeException ex) {
                        LOGGER.warn("Could not write the compute JFR recording during shutdown", ex);
                    } finally {
                        recording.close();
                    }
                }
            }
        } finally {
            SERVICE.shutdownNow();
        }
    }

    private static Result startNow() {
        synchronized (LOCK) {
            if (stopped) {
                return new Result(false, "The compute JFR service is stopped; the server is shutting down.");
            }
            if (!FlightRecorder.isAvailable()) {
                return new Result(false, "Java Flight Recorder is not available in this JVM.");
            }
            if (active != null) {
                return new Result(false, "A Tuff compute recording is already running; stop or dump it first.");
            }
            final Recording recording;
            try {
                recording = new Recording(Configuration.getConfiguration("default"));
            } catch (final IOException | ParseException | RuntimeException ex) {
                LOGGER.error("Could not create the Tuff compute JFR recording", ex);
                return new Result(false, "Could not create the JFR recording: " + ex.getMessage());
            }
            recording.setName(RECORDING_NAME);
            recording.setMaxAge(MAX_AGE);
            recording.setMaxSize(MAX_SIZE_BYTES);
            // enabling the category instead of the registered types also covers event classes that the
            // engine has not loaded yet; Flight Recorder applies the setting when a type registers
            recording.enable(EVENT_CATEGORY);
            try {
                recording.start();
            } catch (final RuntimeException ex) {
                recording.close();
                LOGGER.error("Could not start the Tuff compute JFR recording", ex);
                return new Result(false, "Could not start the JFR recording: " + ex.getMessage());
            }
            active = recording;
            return new Result(true, "Started the JFR recording '" + RECORDING_NAME + "' for the "
                + EVENT_CATEGORY + " events, at most " + MAX_AGE.toMinutes() + " minutes / "
                + (MAX_SIZE_BYTES / (1024L * 1024L)) + " MB. Dump or stop it with /tuff compute jfr.");
        }
    }

    private static Result stopNow() {
        synchronized (LOCK) {
            final @Nullable Recording recording = active;
            if (recording == null) {
                return new Result(false, "No Tuff compute recording is running.");
            }
            active = null;
            try {
                recording.stop();
                final Path file = write(recording);
                return new Result(true, "Stopped the JFR recording and wrote " + describe(file) + ".");
            } catch (final IOException | RuntimeException ex) {
                LOGGER.error("Could not finish the Tuff compute JFR recording", ex);
                return new Result(false, "Could not finish the JFR recording: " + ex.getMessage());
            } finally {
                recording.close();
            }
        }
    }

    private static Result dumpNow() {
        synchronized (LOCK) {
            final @Nullable Recording recording = active;
            if (recording == null) {
                return new Result(false, "No Tuff compute recording is running.");
            }
            try {
                final Path file = write(recording);
                return new Result(true, "Dumped the running JFR recording to " + describe(file) + ".");
            } catch (final IOException | RuntimeException ex) {
                LOGGER.error("Could not dump the Tuff compute JFR recording", ex);
                return new Result(false, "Dumping the JFR recording failed: " + ex.getMessage());
            }
        }
    }

    /**
     * Builds the file name of one recording.
     *
     * @param instant moment the file was written
     * @return a file name with a UTC timestamp, unique to the millisecond
     */
    static String fileName(final Instant instant) {
        return RECORDING_NAME + "-" + FILE_TIMESTAMP.format(instant) + EXTENSION;
    }

    /**
     * Picks a file that does not exist yet.
     *
     * <p>Two dumps in the same millisecond would otherwise produce the same name and the second one
     * would silently overwrite the first; a numeric suffix makes every dump survive. Writes are
     * serialised on {@link #LOCK}, so checking and using the name is not racy.
     *
     * @param directory directory the file will be written to
     * @param instant   moment of the dump
     * @return an unused path inside the directory
     */
    static Path uniqueFile(final Path directory, final Instant instant) {
        final String name = fileName(instant);
        final String stem = name.substring(0, name.length() - EXTENSION.length());
        Path candidate = directory.resolve(name);
        for (int suffix = 1; Files.exists(candidate); suffix++) {
            candidate = directory.resolve(stem + '-' + suffix + EXTENSION);
        }
        return candidate;
    }

    private static Path write(final Recording recording) throws IOException {
        Files.createDirectories(OUTPUT_DIRECTORY);
        final Path file = uniqueFile(OUTPUT_DIRECTORY, Instant.now());
        recording.dump(file);
        return file;
    }

    private static String describe(final Path file) throws IOException {
        final long bytes = Files.size(file);
        return file.toAbsolutePath() + " (" + String.format(Locale.ROOT, "%.1f", bytes / 1024.0d / 1024.0d) + " MB)";
    }

    /**
     * Outcome of one Flight Recorder operation.
     *
     * @param success whether the operation worked
     * @param message sentence for the sender, including the file path when one was written
     */
    record Result(boolean success, String message) {
    }
}
