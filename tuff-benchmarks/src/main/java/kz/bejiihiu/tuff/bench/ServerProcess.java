package kz.bejiihiu.tuff.bench;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * The real server as a child process: one paperclip jar, one working directory, one console pipe.
 *
 * <p>The process is the unit of measurement, not a library call. Everything the analyzer later reads
 * (console output, GC log, JFR recording, exit code) comes from this process, so the harness never
 * invents a number that the server did not produce.
 *
 * <p>Shutdown is graceful: {@code stop} on stdin, then a bounded wait, then a tree kill. A killed run is
 * still recorded, with its real exit code and the reason, and marked invalid by the guardrails.
 */
final class ServerProcess implements AutoCloseable {

    private static final int RING_CAPACITY = 20_000;

    private final Process process;
    private final BufferedWriter log;
    private final BufferedWriter console;
    private final Deque<String> ring = new ArrayDeque<>(RING_CAPACITY);
    private final Object monitor = new Object();
    private final Thread pump;
    private final Path logPath;
    private volatile boolean finished;
    private volatile double stopSeconds = -1.0d;

    private ServerProcess(final Process process, final Path logPath) throws IOException {
        this.process = process;
        this.logPath = logPath;
        Files.createDirectories(logPath.getParent());
        this.log = Files.newBufferedWriter(logPath, StandardCharsets.UTF_8);
        this.console = new BufferedWriter(
            new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8)
        );
        this.pump = Thread.ofPlatform()
            .daemon(true)
            .name("tuff-bench-log-pump")
            .start(this::pumpLog);
    }

    /**
     * Starts the server.
     *
     * @param javaExecutable java of the current toolchain
     * @param jvmFlags       complete flag list, without the executable and the jar
     * @param jar            paperclip jar
     * @param runDir         working directory of the server
     * @return the running process
     * @throws IOException if the process or the log file cannot be created
     */
    static ServerProcess start(
        final Path javaExecutable,
        final List<String> jvmFlags,
        final Path jar,
        final Path runDir
    ) throws IOException {
        final List<String> command = new ArrayList<>(jvmFlags.size() + 4);
        command.add(javaExecutable.toString());
        command.addAll(jvmFlags);
        command.add("-jar");
        command.add(jar.toAbsolutePath().toString());
        command.add("--nogui");
        final Process process = new ProcessBuilder(command)
            .directory(runDir.toFile())
            .redirectErrorStream(true)
            .start();
        return new ServerProcess(process, runDir.resolve("server.log"));
    }

    /**
     * Returns the pid of the server process.
     *
     * @return the pid
     */
    long pid() {
        return this.process.pid();
    }

    /**
     * Sends one console command.
     *
     * @param command command without the leading slash
     * @throws IOException if the pipe is closed
     */
    void send(final String command) throws IOException {
        synchronized (this.monitor) {
            try {
                this.console.write(command);
                this.console.write('\n');
                this.console.flush();
            } catch (final IOException failure) {
                throw new IOException("cannot write '" + command + "' to the server console", failure);
            }
        }
    }

    /**
     * Waits until a console line contains the needle.
     *
     * @param needle  substring to look for
     * @param timeout maximum wait
     * @return {@code true} when the line appeared before the timeout or the process exited
     */
    boolean waitFor(final String needle, final Duration timeout) {
        return this.waitFor(line -> line.contains(needle), timeout);
    }

    /**
     * Waits until a console line matches the predicate.
     *
     * @param predicate line test
     * @param timeout   maximum wait
     * @return {@code true} when a line matched before the timeout
     */
    boolean waitFor(final Predicate<String> predicate, final Duration timeout) {
        final long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (this.monitor) {
            while (true) {
                for (final String line : this.ring) {
                    if (predicate.test(line)) {
                        return true;
                    }
                }
                final long remaining = deadline - System.nanoTime();
                if (remaining <= 0L || this.finished) {
                    return false;
                }
                try {
                    this.monitor.wait(Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remaining)));
                } catch (final InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
    }

    /**
     * Returns whether the server process is still running.
     *
     * @return {@code true} while alive
     */
    boolean alive() {
        return this.process.isAlive();
    }

    /**
     * Sends {@code stop}, waits for the process and kills the tree if it does not leave in time.
     *
     * @param grace how long the server may take
     * @return the observed exit code, or {@code -1} when the wait itself failed
     */
    int stop(final Duration grace) {
        final long started = System.nanoTime();
        try {
            this.send("stop");
        } catch (final IOException failure) {
            this.destroyTree();
            return -1;
        }
        try {
            if (!this.process.waitFor(grace.toMillis(), TimeUnit.MILLISECONDS)) {
                this.destroyTree();
                return this.exitCode();
            }
            return this.exitCode();
        } catch (final InterruptedException failure) {
            Thread.currentThread().interrupt();
            this.destroyTree();
            return -1;
        } finally {
            this.stopSeconds = (System.nanoTime() - started) / 1_000_000_000.0d;
        }
    }

    /**
     * Returns how long the graceful stop took.
     *
     * @return seconds, or {@code -1} when the process was not stopped through this object
     */
    double stopSeconds() {
        return this.stopSeconds;
    }

    /**
     * Returns the exit code, or {@code -1} while the process is still alive.
     *
     * @return the exit code
     */
    int exitCode() {
        return this.process.isAlive() ? -1 : this.process.exitValue();
    }

    /**
     * Kills the process and every descendant.
     *
     * <p>Descendants are killed first so a wrapper or a forked helper cannot outlive the server; the
     * order matters on Windows, where killing the parent alone can leave children running.
     */
    void destroyTree() {
        final List<ProcessHandle> descendants = this.process.descendants().toList();
        for (final ProcessHandle handle : descendants) {
            handle.destroyForcibly();
        }
        this.process.destroyForcibly();
    }

    /**
     * Waits for the log pump to see the end of the stream.
     *
     * @param timeout maximum wait
     * @return {@code true} when the pump finished
     */
    boolean awaitLogEnd(final Duration timeout) {
        try {
            this.pump.join(timeout.toMillis());
            return !this.pump.isAlive();
        } catch (final InterruptedException failure) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Returns the path of the captured console log.
     *
     * @return {@code server.log}
     */
    Path logPath() {
        return this.logPath;
    }

    private void pumpLog() {
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(this.process.getInputStream(), StandardCharsets.UTF_8)
        )) {
            String line;
            while ((line = reader.readLine()) != null) {
                synchronized (this.log) {
                    this.log.write(line);
                    this.log.newLine();
                    this.log.flush();
                }
                synchronized (this.monitor) {
                    if (this.ring.size() >= RING_CAPACITY) {
                        this.ring.removeFirst();
                    }
                    this.ring.addLast(line);
                    this.monitor.notifyAll();
                }
            }
        } catch (final IOException failure) {
            synchronized (this.monitor) {
                this.ring.addLast("[bench] log pump failed: " + failure.getMessage());
                this.monitor.notifyAll();
            }
        } finally {
            synchronized (this.monitor) {
                this.finished = true;
                this.monitor.notifyAll();
            }
        }
    }

    @Override
    public void close() {
        if (this.process.isAlive()) {
            this.destroyTree();
        }
        try {
            this.pump.join(5_000L);
        } catch (final InterruptedException failure) {
            Thread.currentThread().interrupt();
        }
        synchronized (this.log) {
            try {
                this.log.close();
            } catch (final IOException failure) {
                // nothing useful can be done while closing the capture file
            }
        }
    }

    /**
     * Returns a human readable process command line for the manifest.
     *
     * @param javaExecutable java path
     * @param flags          flags
     * @param jar            jar path
     * @return the shell-like command, quoted only where needed
     */
    static String describeCommand(final Path javaExecutable, final List<String> flags, final Path jar) {
        final StringBuilder out = new StringBuilder();
        out.append(quote(javaExecutable.toString()));
        for (final String flag : flags) {
            out.append(' ').append(quote(flag));
        }
        out.append(' ').append("-jar ").append(quote(jar.toAbsolutePath().toString())).append(" --nogui");
        return out.toString();
    }

    private static String quote(final String value) {
        final boolean needsQuotes = value.indexOf(' ') >= 0
            || value.indexOf('\t') >= 0
            || value.indexOf('"') >= 0;
        if (!needsQuotes) {
            return value;
        }
        return '"' + value.replace("\"", "\\\"") + '"';
    }

    /**
     * Returns whether the current platform is Windows.
     *
     * @return {@code true} on Windows
     */
    static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
    }
}
