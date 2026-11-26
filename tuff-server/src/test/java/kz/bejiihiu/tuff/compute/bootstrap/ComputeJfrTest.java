package kz.bejiihiu.tuff.compute.bootstrap;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(30)
class ComputeJfrTest {

    @Test
    void theAnswerIsProducedOffTheThreadThatRanTheCommand() throws Exception {
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch answered = new CountDownLatch(1);
        final AtomicReference<Thread> answerThread = new AtomicReference<>();
        final AtomicReference<ComputeJfr.Result> result = new AtomicReference<>();
        final Thread caller = Thread.currentThread();

        // the operation blocks until the latch is released; if submit() ran it inline this call would
        // never return and the timeout would fail the test
        ComputeJfr.submit("test", () -> {
            boolean released = false;
            try {
                released = release.await(10L, TimeUnit.SECONDS);
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
            return new ComputeJfr.Result(released, released ? "finished" : "still blocked");
        }, value -> {
            answerThread.set(Thread.currentThread());
            result.set(value);
            answered.countDown();
        });

        release.countDown();
        assertTrue(answered.await(10L, TimeUnit.SECONDS), "the operation never reported back");
        assertNotSame(caller, answerThread.get(), "the operation must not run on the calling thread");
        assertTrue(answerThread.get().getName().startsWith("Tuff Compute JFR"), answerThread.get().getName());
        assertTrue(result.get().success(), result.get().message());
        assertEquals("finished", result.get().message());
    }

    @Test
    void twoDumpsInTheSameMillisecondDoNotOverwriteEachOther() throws Exception {
        final Path directory = Files.createTempDirectory("tuff-jfr");
        final Instant instant = Instant.parse("2026-11-26T14:05:12.345Z");

        final Path first = ComputeJfr.uniqueFile(directory, instant);
        Files.createFile(first);
        final Path second = ComputeJfr.uniqueFile(directory, instant);
        Files.createFile(second);
        final Path third = ComputeJfr.uniqueFile(directory, instant);

        assertNotEquals(first, second);
        assertNotEquals(second, third);
        assertEquals("tuff-compute-2026-11-26T14-05-12-345Z.jfr", first.getFileName().toString());
        assertTrue(second.getFileName().toString().endsWith("-1.jfr"), second.getFileName().toString());
        assertTrue(third.getFileName().toString().endsWith("-2.jfr"), third.getFileName().toString());
    }
}
