package kz.bejiihiu.tuff.bench;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Captures a thread dump of the running server with the JDK's own tool.
 *
 * <p>A dump is evidence of what the real process was doing: it shows the scheduler threads, their
 * names, states and stacks, which is exactly what an arm comparison needs when a number looks wrong.
 * The tool is {@code jcmd} from the same JDK that runs the server, so nothing has to be installed.
 *
 * <p>A failed dump never fails the run; the file records why it is empty.
 */
final class ThreadDump {

    private ThreadDump() {
    }

    /**
     * Writes {@code jcmd <pid> Thread.print} into a file.
     *
     * @param javaHome java home of the running JDK
     * @param pid      server pid
     * @param target   destination file
     */
    static void capture(final Path javaHome, final long pid, final Path target) {
        final String executable = System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT)
            .contains("win") ? "jcmd.exe" : "jcmd";
        final Path jcmd = javaHome.resolve("bin").resolve(executable);
        final List<String> command = new ArrayList<>(List.of(
            jcmd.toString(),
            Long.toString(pid),
            "Thread.print"
        ));
        try {
            Files.createDirectories(target.getParent());
            final Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            final String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            if (!process.waitFor(30L, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                Files.writeString(target, "thread dump timed out\n", StandardCharsets.UTF_8);
                return;
            }
            Files.writeString(
                target,
                output.isEmpty() ? "thread dump returned nothing (exit " + process.exitValue() + ")\n" : output,
                StandardCharsets.UTF_8
            );
        } catch (final IOException failure) {
            writeFailure(target, failure);
        } catch (final InterruptedException failure) {
            Thread.currentThread().interrupt();
            writeFailure(target, failure);
        }
    }

    private static void writeFailure(final Path target, final Exception failure) {
        try {
            Files.createDirectories(target.getParent());
            Files.writeString(target, "thread dump failed: " + failure.getMessage() + '\n', StandardCharsets.UTF_8);
        } catch (final IOException ignored) {
            // the artifact is diagnostic only; losing it must not mask the run itself
        }
    }
}
