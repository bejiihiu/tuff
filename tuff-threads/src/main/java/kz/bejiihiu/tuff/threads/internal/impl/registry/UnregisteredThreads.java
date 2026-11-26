package kz.bejiihiu.tuff.threads.internal.impl.registry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadEventSink;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadRegistry;

/**
 * Finds threads that live in the JVM but are not accounted by any pool.
 *
 * <p>This is a diagnostic, not a metric. It takes one JVM wide stack dump, so it belongs to a command,
 * to startup verification or to a periodic audit that runs rarely; the sampler must never call it.
 *
 * <p>Java does not expose the stack a thread was created with. What this report can offer is the stack
 * the thread has at scan time, which usually still names the factory or pool that spawned it. It is
 * kept as evidence, not as a guarantee.
 *
 * <p>Threads known to be outside the manager on purpose (JDK housekeeping, loggers, JFR, profilers,
 * terminal handling) are filtered by {@link #defaultIgnore()}. The final allow list belongs to the
 * static guard package; this default is only what the report uses when nobody passes one.
 */
public final class UnregisteredThreads {

    private static final List<String> IGNORED_PREFIXES = List.of(
        "Reference Handler",
        "Finalizer",
        "Signal Dispatcher",
        "Notification Thread",
        "Common-Cleaner",
        "DestroyJavaVM",
        "Attach Listener",
        "process reaper",
        "log4j2",
        "JFR",
        "spark",
        "JLine",
        "jdk.",
        "JUnit",
        "Test worker",
        "Gradle",
        "ForkJoinPool.commonPool"
    );

    private static final Set<String> IGNORED_GROUPS = Set.of(
        "system",
        "InnocuousThreadGroup"
    );

    private final ThreadRegistry registry;
    private final ThreadEventSink sink;

    /**
     * Creates a report bound to a registry and an event sink.
     *
     * @param registry registry used to decide whether a thread is accounted
     * @param sink     sink notified once per unregistered thread
     * @throws NullPointerException if a reference is {@code null}
     */
    public UnregisteredThreads(final ThreadRegistry registry, final ThreadEventSink sink) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.sink = Objects.requireNonNull(sink, "sink");
    }

    /**
     * Returns the allow list this report uses when no predicate is supplied.
     *
     * @return a predicate that ignores known outside threads
     */
    public static Predicate<Thread> defaultIgnore() {
        return thread -> {
            final ThreadGroup group = thread.getThreadGroup();
            if (group != null && IGNORED_GROUPS.contains(group.getName())) {
                return true;
            }
            if (thread == Thread.currentThread()) {
                return true;
            }
            final String name = thread.getName();
            for (final String prefix : IGNORED_PREFIXES) {
                if (name.startsWith(prefix) || name.contains(prefix)) {
                    return true;
                }
            }
            return false;
        };
    }

    /**
     * Scans the JVM with the default allow list.
     *
     * @return unregistered threads in thread id order
     */
    public List<Entry> scan() {
        return this.scan(defaultIgnore());
    }

    /**
     * Scans the JVM for threads that are neither registered nor ignored.
     *
     * <p>One stack dump is taken for all live threads; stacks of registered threads are discarded.
     * Every reported thread also goes to the event sink as {@code unregisteredThread}.
     *
     * @param ignored predicate that returns {@code true} for threads to leave out
     * @return unregistered threads in thread id order
     * @throws NullPointerException if {@code ignored} is {@code null}
     */
    public List<Entry> scan(final Predicate<Thread> ignored) {
        Objects.requireNonNull(ignored, "ignored");

        final Map<Thread, StackTraceElement[]> stacks = Thread.getAllStackTraces();
        final List<Entry> found = new ArrayList<>();
        for (final Map.Entry<Thread, StackTraceElement[]> candidate : stacks.entrySet()) {
            final Thread thread = candidate.getKey();
            if (this.registry.poolOf(thread).isPresent() || ignored.test(thread)) {
                continue;
            }
            final ThreadGroup group = thread.getThreadGroup();
            found.add(new Entry(
                thread.threadId(),
                thread.getName(),
                thread.isDaemon(),
                group == null ? "unknown" : group.getName(),
                List.of(candidate.getValue())
            ));
            this.emit(thread);
        }
        found.sort(Comparator.comparingLong(Entry::threadId));
        return List.copyOf(found);
    }

    private void emit(final Thread thread) {
        try {
            this.sink.unregisteredThread(thread.threadId(), thread.getName());
        } catch (RuntimeException _) {
            // a diagnostic must never take the caller down
        }
    }

    /**
     * One unaccounted thread.
     *
     * @param threadId    JVM thread id
     * @param name        thread name at scan time
     * @param daemon      whether the thread is a daemon thread
     * @param threadGroup name of the thread group, or {@code "unknown"}
     * @param stackAtScan stack captured while scanning, empty if the JVM refused it
     */
    public record Entry(
        long threadId,
        String name,
        boolean daemon,
        String threadGroup,
        List<StackTraceElement> stackAtScan
    ) {
    }
}
