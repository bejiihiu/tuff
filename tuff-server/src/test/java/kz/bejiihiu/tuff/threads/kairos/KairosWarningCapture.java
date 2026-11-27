package kz.bejiihiu.tuff.threads.kairos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.jspecify.annotations.Nullable;

/**
 * Records WARN and above from the Kairos logger, which is exactly the stream the no-spam promises are
 * about.
 *
 * <p>Attached to the live logger context, so it sees the same lines an operator would. A test class
 * path without the log4j core implementation gets {@code null} from {@link #attach()} and should skip
 * instead of failing.
 */
final class KairosWarningCapture implements AutoCloseable {

    private final LoggerContext context;
    private final RecordingAppender appender;

    private KairosWarningCapture(final LoggerContext context, final RecordingAppender appender) {
        this.context = context;
        this.appender = appender;
    }

    /**
     * Attaches a recorder to the root logger.
     *
     * @return the capture, or {@code null} when the logging backend cannot be inspected
     */
    static @Nullable KairosWarningCapture attach() {
        if (!(LogManager.getContext(false) instanceof LoggerContext context)) {
            return null;
        }
        final RecordingAppender appender = new RecordingAppender();
        appender.start();
        context.getRootLogger().addAppender(appender);
        context.updateLoggers();
        return new KairosWarningCapture(context, appender);
    }

    /**
     * Waits until the requested number of lines was recorded.
     *
     * @param expected   number of lines to wait for
     * @param waitMillis budget of the wait
     * @return {@code true} when the count was reached
     * @throws InterruptedException if the waiting thread is interrupted
     */
    boolean await(final int expected, final long waitMillis) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMillis);
        while (this.appender.events.size() < expected) {
            if (System.nanoTime() - deadline >= 0L) {
                return false;
            }
            Thread.sleep(10L);
        }
        return true;
    }

    /**
     * Returns the lines recorded so far, in order.
     *
     * @return an immutable list, possibly empty
     */
    List<String> warnings() {
        return List.copyOf(this.appender.events);
    }

    @Override
    public void close() {
        this.context.getRootLogger().removeAppender(this.appender);
        this.context.updateLoggers();
        this.appender.stop();
    }

    private static final class RecordingAppender extends AbstractAppender {

        private final List<String> events = Collections.synchronizedList(new ArrayList<>());

        private RecordingAppender() {
            super("KairosWarningCapture", null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(final LogEvent event) {
            // lower intLevel is more severe: WARN and only what is worse than it
            if (event.getLevel().intLevel() > Level.WARN.intLevel()) {
                return;
            }
            if (!"Tuff/Kairos".equals(event.getLoggerName())) {
                return;
            }
            this.events.add(event.getMessage().getFormattedMessage());
        }
    }
}
