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
 * <p>All operations are serialised on one lock, because only one Tuff recording may exist at a time;
 * holding it also keeps the Flight Recorder calls off each other. A recording that is still running
 * when the server stops is written out from {@link #shutdown()}, so an administrator who forgot to
 * stop it still gets the data.
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
    private static final DateTimeFormatter FILE_TIMESTAMP =
        DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH-mm-ss-SSS'Z'", Locale.ROOT).withZone(ZoneOffset.UTC);
    private static final Logger LOGGER = LogManager.getLogger("Tuff/Compute");

    private static final Object LOCK = new Object();

    private static @Nullable Recording active;

    private ComputeJfr() {
    }

    /**
     * Starts a recording.
     *
     * @return what happened, ready to be shown to the sender
     */
    static Result start() {
        synchronized (LOCK) {
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

    /**
     * Stops the recording and writes what it captured.
     *
     * @return what happened, ready to be shown to the sender
     */
    static Result stop() {
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

    /**
     * Copies the running recording to a file without stopping it.
     *
     * @return what happened, ready to be shown to the sender
     */
    static Result dump() {
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
     * Stops and writes an active recording during server shutdown.
     *
     * <p>Never throws: shutdown must continue even when the recording cannot be written.
     */
    static void shutdown() {
        synchronized (LOCK) {
            final @Nullable Recording recording = active;
            if (recording == null) {
                return;
            }
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

    /**
     * Builds the file name of one recording.
     *
     * @param instant moment the file was written
     * @return a file name with a UTC timestamp, unique to the millisecond
     */
    static String fileName(final Instant instant) {
        return RECORDING_NAME + "-" + FILE_TIMESTAMP.format(instant) + ".jfr";
    }

    private static Path write(final Recording recording) throws IOException {
        Files.createDirectories(OUTPUT_DIRECTORY);
        final Path file = OUTPUT_DIRECTORY.resolve(fileName(Instant.now()));
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
