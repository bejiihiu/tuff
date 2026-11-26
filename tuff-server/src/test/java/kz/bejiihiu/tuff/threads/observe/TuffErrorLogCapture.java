package kz.bejiihiu.tuff.threads.observe;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.jspecify.annotations.Nullable;

/**
 * Records the ERROR and FATAL lines this project logs while a test runs.
 *
 * <p>The observe-only promise includes "a failure is visible but not loud": a layer that starts fine
 * must not log an error on the way, and a probe that reads an absent scheduler must not treat that as
 * a failure. Reading the log stream is the cheap way to check that, so a test attaches this appender
 * and looks at what arrived.
 *
 * <p>Only logger names starting with {@code Tuff} are recorded. Everything else on the root logger -
 * the test runner, the JDK, third party libraries - is ignored on purpose: a noisy neighbour must not
 * make a test red.
 *
 * <p>Only available when log4j2 core is on the test classpath, which it is for the server module.
 * {@link #attach()} answers {@code null} otherwise and the caller skips the check.
 */
public final class TuffErrorLogCapture implements AutoCloseable {

    private static final String PREFIX = "Tuff";

    private final LoggerContext context;
    private final RecordingAppender appender;

    private TuffErrorLogCapture(final LoggerContext context, final RecordingAppender appender) {
        this.context = context;
        this.appender = appender;
    }

    /**
     * Attaches the recorder to the root logger.
     *
     * @return the capture, or {@code null} when there is no log4j2 core to attach to
     */
    public static @Nullable TuffErrorLogCapture attach() {
        if (!(LogManager.getContext(false) instanceof LoggerContext context)) {
            return null;
        }
        final RecordingAppender appender = new RecordingAppender();
        appender.start();
        context.getRootLogger().addAppender(appender);
        context.updateLoggers();
        return new TuffErrorLogCapture(context, appender);
    }

    /**
     * Returns the recorded events, formatted as {@code logger: message}.
     *
     * @return an immutable list, empty when nothing was logged at ERROR or above
     */
    public List<String> errors() {
        return List.copyOf(this.appender.events);
    }

    @Override
    public void close() {
        this.context.getRootLogger().removeAppender(this.appender);
        this.context.updateLoggers();
        this.appender.stop();
    }

    /**
     * Appender that keeps the interesting events in memory.
     */
    private static final class RecordingAppender extends AbstractAppender {

        private final List<String> events = new CopyOnWriteArrayList<>();

        private RecordingAppender() {
            super("TuffErrorLogCapture", null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(final LogEvent event) {
            // lower intLevel is more severe in log4j2: ERROR and FATAL, nothing else
            if (event.getLevel().intLevel() > Level.ERROR.intLevel()) {
                return;
            }
            final String logger = event.getLoggerName();
            if (logger == null || !logger.startsWith(PREFIX)) {
                return;
            }
            this.events.add(logger + ": " + event.getMessage().getFormattedMessage());
        }
    }
}
