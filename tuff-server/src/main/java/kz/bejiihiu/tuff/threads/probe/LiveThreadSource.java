package kz.bejiihiu.tuff.threads.probe;

import java.util.ArrayList;
import java.util.List;

/**
 * Source of the live platform threads of the process.
 *
 * <p>Only used by probes that have no pool API to ask: netty names its event loops, the chat executor
 * exposes counters but not its threads and the background worker pool is only reachable through names.
 * The scan walks the root {@link ThreadGroup} once, which is cheap, and never takes a stack dump.
 *
 * <p>Tests inject a plain list; every implementation is called from the sampler thread and must not
 * block.
 */
@FunctionalInterface
interface LiveThreadSource {

    /**
     * Returns every live thread, unordered.
     *
     * @return the threads, possibly empty
     */
    List<Thread> threads();

    /**
     * Returns the source that reads the running JVM.
     *
     * @return a root group scan
     */
    static LiveThreadSource jvm() {
        return LiveThreadSource::rootThreads;
    }

    /**
     * Enumerates the root group recursively.
     *
     * <p>The buffer grows instead of being trusted: thread creation can race the scan and a fixed
     * buffer would silently drop the newest threads.
     *
     * @return the live threads
     */
    private static List<Thread> rootThreads() {
        ThreadGroup group = Thread.currentThread().getThreadGroup();
        while (group.getParent() != null) {
            group = group.getParent();
        }
        Thread[] buffer = new Thread[group.activeCount() + 16];
        int count = group.enumerate(buffer, true);
        while (count == buffer.length) {
            buffer = new Thread[buffer.length * 2];
            count = group.enumerate(buffer, true);
        }
        final List<Thread> threads = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            if (buffer[index] != null) {
                threads.add(buffer[index]);
            }
        }
        return threads;
    }
}
