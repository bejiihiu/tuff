package kz.bejiihiu.tuff.threads.internal.impl.registry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadEventSink;
import kz.bejiihiu.tuff.threads.internal.spi.ThreadRegistry;

/**
 * Finds threads that live in the JVM but are not accounted by any pool.
 *
 * <p>This is a diagnostic, not a metric. The walk itself is cheap (one enumeration of the root thread
 * group), but {@link #scan(Predicate)} also captures a stack per finding, so it belongs to a command, to
 * startup verification or to a periodic audit that runs rarely; the sampler must never call it. The
 * sampler uses {@link #unaccounted(ThreadRegistry, Predicate)}, which is the same walk and the same
 * allow list without the stacks.
 *
 * <p>Java does not expose the stack a thread was created with. What the report can offer is the stack
 * the thread has at scan time, which usually still names the factory or pool that spawned it. It is
 * kept as evidence, not as a guarantee.
 *
 * <p>Threads known to be outside the manager on purpose (JDK housekeeping, loggers, JFR, profilers,
 * terminal handling) are filtered by {@link #defaultIgnore()}. That is the module's only allow list; the
 * sampler shares it instead of keeping its own copy.
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
     * <p>Uses {@link #unaccounted(ThreadRegistry, Predicate)} for the walk, then keeps the stack every
     * reported thread has at scan time. Every reported thread also goes to the event sink as
     * {@code unregisteredThread}.
     *
     * @param ignored predicate that returns {@code true} for threads to leave out
     * @return unregistered threads in thread id order
     * @throws NullPointerException if {@code ignored} is {@code null}
     */
    public List<Entry> scan(final Predicate<Thread> ignored) {
        final List<Entry> found = new ArrayList<>();
        for (final Thread thread : unaccounted(this.registry, ignored)) {
            final ThreadGroup group = thread.getThreadGroup();
            found.add(new Entry(
                thread.threadId(),
                thread.getName(),
                thread.isDaemon(),
                group == null ? "unknown" : group.getName(),
                List.of(thread.getStackTrace())
            ));
            this.emit(thread);
        }
        return List.copyOf(found);
    }

    /**
     * Returns live threads that no pool accounts for, without taking any stack.
     *
     * <p>This is the one implementation of the walk and the filter: {@link #scan(Predicate)} layers the
     * stack evidence on top of it, and the metrics sampler uses it directly so both reports can never
     * disagree about which thread is unaccounted.
     *
     * @param registry registry used to decide whether a thread is accounted
     * @param ignored  predicate that returns {@code true} for threads to leave out
     * @return the threads, in thread id order
     * @throws NullPointerException if a reference is {@code null}
     */
    public static List<Thread> unaccounted(final ThreadRegistry registry, final Predicate<Thread> ignored) {
        Objects.requireNonNull(registry, "registry");
        Objects.requireNonNull(ignored, "ignored");
        final List<Thread> found = new ArrayList<>();
        for (final Thread thread : liveThreads()) {
            if (registry.poolOf(thread).isEmpty() && !ignored.test(thread)) {
                found.add(thread);
            }
        }
        found.sort(Comparator.comparingLong(Thread::threadId));
        return List.copyOf(found);
    }

    /**
     * Enumerates every live thread of the JVM.
     *
     * @return the threads, unordered
     */
    private static List<Thread> liveThreads() {
        ThreadGroup root = Thread.currentThread().getThreadGroup();
        while (root.getParent() != null) {
            root = root.getParent();
        }
        Thread[] buffer = new Thread[root.activeCount() + 16];
        int count = root.enumerate(buffer, true);
        while (count == buffer.length) {
            buffer = new Thread[buffer.length * 2];
            count = root.enumerate(buffer, true);
        }
        final List<Thread> threads = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            threads.add(buffer[index]);
        }
        return threads;
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
