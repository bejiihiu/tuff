package kz.bejiihiu.tuff.threads.kairos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Builds plain daemon threads and keeps every one of them, so a test can prove that none leaked.
 */
final class RecordingThreadFactory implements ThreadFactory {

    private static final AtomicInteger GENERATIONS = new AtomicInteger();

    private final AtomicInteger ids = new AtomicInteger();
    private final String prefix;
    private final List<Thread> created = Collections.synchronizedList(new ArrayList<>());

    RecordingThreadFactory() {
        this.prefix = "kairos-test-" + GENERATIONS.getAndIncrement();
    }

    @Override
    public Thread newThread(final Runnable run) {
        final Thread thread = new Thread(run, this.prefix + "-" + this.ids.getAndIncrement());
        thread.setDaemon(true);
        this.created.add(thread);
        return thread;
    }

    List<Thread> created() {
        return List.copyOf(this.created);
    }

    boolean anyAlive() {
        return this.created.stream().anyMatch(Thread::isAlive);
    }
}
