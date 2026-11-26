package kz.bejiihiu.tuff.compute.bootstrap;

import io.papermc.paper.threadedregions.RegionizedServer;
import java.util.List;
import kz.bejiihiu.tuff.compute.AsyncComputeScheduler;
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
 * The {@code /tuff} server command: engine summary, per consumer breakdown, configuration reload and
 * Flight Recorder control.
 *
 * <p>Registered through the Bukkit command map next to {@code /tps}, so it survives {@code /reload}
 * and works from the console and from players alike. Reading metrics is lock free and can happen on
 * whatever thread the sender is on; the reload hops to the global tick thread because it mutates
 * shared engine state, exactly like the Paper reload command does. Flight Recorder operations run on
 * their own service thread and report back when they finish, so a dump never stalls the region that
 * ran the command. A disabled engine is never an error: the command says so and returns.
 *
 * <p>Permission {@code tuff.command.compute}, granted to operators by default.
 */
public final class TuffCommand extends Command implements TabCompleter {

    /**
     * Permission required to use any part of the command.
     */
    public static final String PERMISSION = "tuff.command.compute";

    /**
     * Creates the command and registers its permission when that has not happened yet.
     *
     * @return a command ready to be registered in the command map
     */
    public static TuffCommand create() {
        registerPermission();
        return new TuffCommand();
    }

    private TuffCommand() {
        super("tuff", "Inspects and controls the Tuff compute engine.", ComputeCommand.USAGE, List.of());
        this.setPermission(PERMISSION);
        this.permissionMessage(Component.text(
            "You do not have permission to inspect the Tuff compute engine.", NamedTextColor.RED));
    }

    @Override
    public boolean execute(final CommandSender sender, final String commandLabel, final String[] args) {
        if (!this.testPermissionSilent(sender)) {
            sender.sendMessage(Component.text(
                "You do not have permission to inspect the Tuff compute engine.", NamedTextColor.RED));
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
        switch (action) {
            case SUMMARY -> this.send(sender, ComputeReport.overview(ComputeBootstrap.status()));
            case CONSUMERS -> this.send(sender, ComputeReport.consumers(ComputeBootstrap.status()));
            case TASKS -> this.send(sender, ComputeReport.tasks(ComputeBootstrap.status(), request.limit()));
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
        if (!this.testPermissionSilent(sender)) {
            return List.of();
        }
        return ComputeCommand.completions(args);
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
        if (pluginManager == null || pluginManager.getPermission(PERMISSION) != null) {
            return;
        }
        pluginManager.addPermission(new Permission(
            PERMISSION,
            "Allows inspecting, reloading and profiling the Tuff compute engine.",
            PermissionDefault.OP
        ));
    }
}
