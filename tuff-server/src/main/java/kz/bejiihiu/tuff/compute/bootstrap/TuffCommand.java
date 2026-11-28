package kz.bejiihiu.tuff.compute.bootstrap;

import io.papermc.paper.threadedregions.RegionizedServer;
import java.util.ArrayList;
import java.util.List;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
import kz.bejiihiu.tuff.compute.ComputeSettings;
import kz.bejiihiu.tuff.threads.ThreadManagementSettings;
import kz.bejiihiu.tuff.threads.bootstrap.ThreadsBootstrap;
import kz.bejiihiu.tuff.threads.bootstrap.ThreadsReport;
import kz.bejiihiu.tuff.threads.bootstrap.ThreadsStatus;
import kz.bejiihiu.tuff.threads.kairos.KairosReport;
import kz.bejiihiu.tuff.threads.kairos.KairosScheduler;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;
import net.kyori.adventure.text.format.TextDecoration;
import net.minecraft.server.MinecraftServer;
import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.command.TabCompleter;
import org.bukkit.permissions.Permission;
import org.bukkit.permissions.PermissionDefault;
import org.bukkit.plugin.PluginManager;
import org.jspecify.annotations.Nullable;

/**
 * The {@code /tuff} server command: engine summary, per consumer breakdown, configuration reload,
 * Flight Recorder control, the thread layer summary and the region scheduler report.
 *
 * <p>Registered through the Bukkit command map next to {@code /tps}, so it survives {@code /reload}
 * and works from the console and from players alike. Reading metrics is lock free and can happen on
 * whatever thread the sender is on; the reload hops to the global tick thread because it mutates
 * shared engine state, exactly like the Paper reload command does. Flight Recorder operations run on
 * their own service thread and report back when they finish, so a dump never stalls the region that
 * ran the command. A disabled engine is never an error: the command says so and returns.
 *
 * <p>Permission {@code tuff.command.compute}, granted to operators by default, is the control
 * permission and covers every form of the command. {@code tuff.command.scheduler} is the read-only
 * diagnostics permission: {@code threads} and the scheduler overview with its {@code leases},
 * {@code budget} and {@code help} views. A sender that holds only the read-only permission is refused
 * everything else - the compute views, {@code reload}, the Flight Recorder verbs and
 * {@code scheduler reset} - with a message naming the permission those actions need, and tab
 * completion only offers the words that sender may run.
 */
public final class TuffCommand extends Command implements TabCompleter {

    /**
     * Permission required to control the engine: reload, Flight Recorder and the scheduler reset.
     * It also grants every read-only view, so operators need only this one.
     */
    public static final String PERMISSION = "tuff.command.compute";

    /**
     * Permission required for the read-only diagnostics: the thread layer summary and the region
     * scheduler report with its leases, budget and help views. Granted to operators by default and
     * kept apart from the compute permission so a server can hand the diagnostics to a wider group
     * without also handing over reload, the Flight Recorder verbs, the scheduler counter reset or the
     * compute views.
     */
    public static final String SCHEDULER_PERMISSION = "tuff.command.scheduler";

    /**
     * Root subcommands the read-only permission may run.
     */
    private static final List<String> READ_ONLY_ROOT = List.of("threads", "scheduler");

    /**
     * Scheduler subcommands the read-only permission may run.
     */
    private static final List<String> READ_ONLY_SCHEDULER = List.of("leases", "budget", "help");

    /**
     * Message for a sender without either permission.
     */
    private static final String NO_PERMISSION = "You do not have permission to use the Tuff command.";

    /**
     * Creates the command and registers its two permissions when that has not happened yet.
     *
     * @return a command ready to be registered in the command map
     */
    public static TuffCommand create() {
        registerPermission();
        return new TuffCommand();
    }

    /**
     * Builds the command.
     *
     * <p>Package private so the permission matrix can be exercised without a running server;
     * {@link #create()} is the entry point that production code uses and it also registers the two
     * permissions.
     */
    TuffCommand() {
        super("tuff", "Inspects and controls the Tuff compute engine.", ComputeCommand.USAGE, List.of());
        this.setPermission(PERMISSION);
        this.permissionMessage(Component.text(NO_PERMISSION, NamedTextColor.RED));
    }

    @Override
    public boolean execute(final CommandSender sender, final String commandLabel, final String[] args) {
        if (!this.allowed(sender)) {
            sender.sendMessage(Component.text(NO_PERMISSION, NamedTextColor.RED));
            return true;
        }
        final ComputeCommand.Request request = ComputeCommand.parse(args);
        if (request.failed()) {
            sender.sendMessage(Component.text(request.error(), NamedTextColor.RED));
            return true;
        }
        final ComputeCommand.Action action = request.action();
        if (action == null) {
            sender.sendMessage(Component.text("Usage: " + ComputeCommand.USAGE, NamedTextColor.RED));
            return true;
        }
        if (!readOnly(action) && !this.mayControl(sender)) {
            sender.sendMessage(Component.text(denyPermission(action), NamedTextColor.RED));
            return true;
        }
        switch (action) {
            case SUMMARY -> this.send(sender, ComputeReport.overview(ComputeBootstrap.status()));
            case CONSUMERS -> this.send(sender, ComputeReport.consumers(ComputeBootstrap.status()));
            case TASKS -> this.send(sender, ComputeReport.tasks(ComputeBootstrap.status(), request.limit()));
            case THREADS -> this.send(sender, ThreadsReport.overview(ThreadsBootstrap.status()));
            case SCHEDULER -> this.sendScheduler(sender);
            case SCHEDULER_LEASES -> this.sendLeases(sender, request.limit());
            case SCHEDULER_BUDGET -> this.sendBudget(sender);
            case SCHEDULER_RESET -> this.resetScheduler(sender);
            case SCHEDULER_HELP -> this.send(sender, KairosReport.help());
            case RELOAD -> this.reload(sender);
            case JFR_START -> this.startRecording(sender);
            case JFR_STOP, JFR_DUMP -> this.controlRecording(sender, action == ComputeCommand.Action.JFR_STOP);
        }
        return true;
    }

    @Override
    public List<String> tabComplete(
        final CommandSender sender,
        final String alias,
        final String[] args
    ) throws IllegalArgumentException {
        if (!this.allowed(sender)) {
            return List.of();
        }
        final List<String> candidates = ComputeCommand.completions(args);
        if (this.mayControl(sender) || candidates.isEmpty()) {
            return candidates;
        }
        return readOnly(candidates, args);
    }

    /**
     * {@inheritDoc}
     *
     * <p>{@link TabCompleter} is obsolete since 26.3 and the command map uses
     * {@link Command#tabComplete} instead, but the delegating implementation keeps both entry points
     * answering the same way for callers that still go through the old interface.
     */
    @Override
    public List<String> onTabComplete(
        final CommandSender sender,
        final Command command,
        final String label,
        final String[] args
    ) {
        return this.tabComplete(sender, label, args);
    }

    private void sendScheduler(final CommandSender sender) {
        final ThreadsStatus status = ThreadsBootstrap.status();
        final ThreadManagementSettings settings = status.settings();
        final int configured = settings == null ? -1 : settings.regionExecution().threads();
        this.send(sender, KairosReport.overview(activeScheduler(status), configured, status.kairos()));
    }

    private void sendLeases(final CommandSender sender, final int limit) {
        final ThreadsStatus status = ThreadsBootstrap.status();
        this.send(sender, KairosReport.leases(activeScheduler(status), status.kairos(), limit));
    }

    private void sendBudget(final CommandSender sender) {
        final ThreadsStatus status = ThreadsBootstrap.status();
        final ComputeSettings.@Nullable Workers workers = ComputeBootstrap.status().engine()
            .map(engine -> engine.settings().workers())
            .orElse(null);
        this.send(sender, KairosReport.budget(new KairosReport.BudgetInput(
            activeScheduler(status),
            status.snapshot(),
            status.sampleAgeMillis(),
            status.snapshot() == null ? describeMissingSample(status) : null,
            status.kairos(),
            workers == null ? 0 : workers.minThreads(),
            workers == null ? -1 : workers.maxThreads()
        )));
    }

    private void resetScheduler(final CommandSender sender) {
        final boolean applied = KairosScheduler.resetActiveCounters();
        this.send(sender, KairosReport.reset(applied, activeScheduler(ThreadsBootstrap.status())));
    }

    /**
     * Names the active region scheduler, or {@code null} when the thread layer cannot name one.
     *
     * @param status current thread layer status
     * @return {@code KAIROS} while a fund exists, the configured name otherwise
     */
    private static @Nullable String activeScheduler(final ThreadsStatus status) {
        if (status.kairos() != null) {
            return "KAIROS";
        }
        final ThreadManagementSettings settings = status.settings();
        return settings == null ? null : settings.regionExecution().scheduler().name();
    }

    /**
     * Explains why the thread layer has no sample to read.
     *
     * @param status current thread layer status
     * @return one clause for the budget view
     */
    private static String describeMissingSample(final ThreadsStatus status) {
        return switch (status.state()) {
            case NOT_INITIALISED -> "not started yet; the ledger, the deductions and usable cores are n/a "
                + "until the thread layer starts";
            case DISABLED -> "disabled (thread-management.enabled: false); the ledger, the deductions and "
                + "usable cores are n/a";
            case FAILED -> "failed to start (the cause is in the Tuff/Threads log); the ledger, the "
                + "deductions and usable cores are n/a";
            case RUNNING -> "running, no sample yet (the refresher takes one pass per second); the ledger, "
                + "the deductions and usable cores are n/a until it publishes one";
        };
    }

    private boolean allowed(final CommandSender sender) {
        return this.mayControl(sender) || sender.hasPermission(SCHEDULER_PERMISSION);
    }

    private boolean mayControl(final CommandSender sender) {
        return this.testPermissionSilent(sender);
    }

    /**
     * Checks whether the read-only scheduler permission covers an action.
     *
     * <p>The read-only permission is the thread layer summary and the scheduler report with its
     * {@code leases}, {@code budget} and {@code help} views. Everything else - the compute views,
     * {@code reload}, the three Flight Recorder verbs and {@code scheduler reset} - needs
     * {@code tuff.command.compute}.
     *
     * @param action parsed action
     * @return {@code true} when {@code tuff.command.scheduler} alone may run the action
     */
    private static boolean readOnly(final ComputeCommand.Action action) {
        return switch (action) {
            case THREADS, SCHEDULER, SCHEDULER_LEASES, SCHEDULER_BUDGET, SCHEDULER_HELP -> true;
            case SUMMARY, CONSUMERS, TASKS, RELOAD, JFR_START, JFR_STOP, JFR_DUMP, SCHEDULER_RESET -> false;
        };
    }

    /**
     * Builds the refusal for a sender that only holds the read-only permission.
     *
     * @param action the refused action
     * @return one line naming the form and the permission it needs
     */
    private static String denyPermission(final ComputeCommand.Action action) {
        final String form = switch (action) {
            case SUMMARY, CONSUMERS, TASKS -> "/tuff compute";
            case RELOAD -> "/tuff reload";
            case JFR_START -> "/tuff compute jfr start";
            case JFR_STOP -> "/tuff compute jfr stop";
            case JFR_DUMP -> "/tuff compute jfr dump";
            case SCHEDULER_RESET -> "/tuff scheduler reset";
            case THREADS, SCHEDULER, SCHEDULER_LEASES, SCHEDULER_BUDGET, SCHEDULER_HELP -> "/tuff";
        };
        return "You do not have permission to run " + form + ": it needs " + PERMISSION
            + ", and " + SCHEDULER_PERMISSION + " only covers the read-only thread and scheduler views.";
    }

    /**
     * Removes the words a read-only sender may not run from a completion list.
     *
     * <p>The parser owns the grammar; this only keeps the branches the scheduler permission covers, so
     * the two lists below are the same two statements the refusal and the docs make.
     *
     * @param candidates completions of the current position
     * @param args       arguments as Bukkit passes them
     * @return the completions that sender may run
     */
    private static List<String> readOnly(final List<String> candidates, final String[] args) {
        final List<String> permitted = switch (args.length - 1) {
            case 0 -> READ_ONLY_ROOT;
            case 1 -> args[0].equalsIgnoreCase("scheduler") ? READ_ONLY_SCHEDULER : List.of();
            default -> List.of();
        };
        final List<String> visible = new ArrayList<>(permitted.size());
        for (final String candidate : candidates) {
            if (permitted.contains(candidate)) {
                visible.add(candidate);
            }
        }
        return List.copyOf(visible);
    }

    private void reload(final CommandSender sender) {
        final ComputeStatus status = ComputeBootstrap.status();
        if (!status.isRunning()) {
            this.send(sender, ComputeReport.unavailable(status));
            return;
        }
        final @Nullable MinecraftServer server = MinecraftServer.getServer();
        if (server == null || !server.isRunning()) {
            sender.sendMessage(Component.text(
                "The server is stopping, so the compute configuration was not reloaded.", NamedTextColor.RED));
            return;
        }
        sender.sendMessage(Component.text("Reloading the compute configuration...", NamedTextColor.GRAY));
        RegionizedServer.getInstance().addTask(() -> {
            final @Nullable ReloadReport report = ComputeBootstrap.reload();
            if (report == null) {
                sender.sendMessage(Component.text(
                    "The compute engine is no longer running; nothing was reloaded.", NamedTextColor.RED));
                return;
            }
            this.send(sender, ComputeReport.reload(report));
        });
    }

    private void startRecording(final CommandSender sender) {
        final ComputeStatus status = ComputeBootstrap.status();
        if (!status.isRunning()) {
            this.send(sender, ComputeReport.unavailable(status));
            return;
        }
        final @Nullable AsyncComputeScheduler scheduler = status.engine().orElse(null);
        if (scheduler != null && !scheduler.settings().metrics().jfrEnabled()) {
            sender.sendMessage(Component.text(
                "JFR events are disabled for the compute engine (async-compute.metrics.jfr: false), so the "
                    + "recording would stay empty. Enable them and restart the server first.", NamedTextColor.RED));
            return;
        }
        sender.sendMessage(Component.text("Starting the JFR recording...", NamedTextColor.GRAY));
        ComputeJfr.start(result -> this.sendResult(sender, result));
    }

    private void controlRecording(final CommandSender sender, final boolean stop) {
        final ComputeStatus status = ComputeBootstrap.status();
        if (!status.isRunning()) {
            this.send(sender, ComputeReport.unavailable(status));
            return;
        }
        sender.sendMessage(Component.text(
            stop ? "Stopping the JFR recording..." : "Dumping the JFR recording...", NamedTextColor.GRAY));
        if (stop) {
            ComputeJfr.stop(result -> this.sendResult(sender, result));
        } else {
            ComputeJfr.dump(result -> this.sendResult(sender, result));
        }
    }

    private void sendResult(final CommandSender sender, final ComputeJfr.Result result) {
        sender.sendMessage(Component.text(result.message(), result.success() ? NamedTextColor.GREEN : NamedTextColor.RED));
    }

    private void send(final CommandSender sender, final List<String> lines) {
        if (lines.isEmpty()) {
            return;
        }
        Component message = Component.text(lines.get(0), NamedTextColor.AQUA, TextDecoration.BOLD);
        for (int index = 1; index < lines.size(); index++) {
            message = message.append(Component.newline()).append(Component.text(lines.get(index), NamedTextColor.GRAY));
        }
        sender.sendMessage(message);
    }

    private static void registerPermission() {
        final PluginManager pluginManager = Bukkit.getPluginManager();
        if (pluginManager == null) {
            return;
        }
        if (pluginManager.getPermission(PERMISSION) == null) {
            pluginManager.addPermission(new Permission(
                PERMISSION,
                "Allows inspecting, reloading and profiling the Tuff compute engine and thread layer.",
                PermissionDefault.OP
            ));
        }
        if (pluginManager.getPermission(SCHEDULER_PERMISSION) == null) {
            pluginManager.addPermission(new Permission(
                SCHEDULER_PERMISSION,
                "Allows reading the Tuff thread layer and region scheduler reports; the compute views, "
                    + "reload, Flight Recorder and the scheduler counter reset need tuff.command.compute.",
                PermissionDefault.OP
            ));
        }
    }
}
