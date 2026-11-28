package kz.bejiihiu.tuff.compute.bootstrap;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import net.kyori.adventure.text.Component;
import org.bukkit.Server;
import org.bukkit.command.CommandSender;
import org.bukkit.permissions.Permission;
import org.bukkit.permissions.PermissionAttachment;
import org.bukkit.permissions.PermissionAttachmentInfo;
import org.bukkit.plugin.Plugin;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The permission matrix of the command.
 *
 * <p>{@code tuff.command.compute} is the control permission and {@code tuff.command.scheduler} is the
 * read-only one. A sender holding only the second must reach every diagnostics view and none of the
 * mutating forms; a sender holding neither must reach nothing. The sender is hand-written instead of
 * mocked so the assertions read as the contract: which question is asked of the sender, and what it
 * is told back.
 *
 * <p>No engine or thread layer is started. The command falls back to its "not initialised" answers,
 * which is enough to prove the gate ran: the point of every case here is the permission decision, not
 * the report it would print on a live server.
 */
@Timeout(30)
class TuffCommandPermissionTest {

    private final TuffCommand command = new TuffCommand();

    @Test
    void aSchedulerOnlySenderReadsEveryDiagnosticsView() {
        final FakeSender sender = sender(TuffCommand.SCHEDULER_PERMISSION);

        assertReducesTo("Tuff scheduler: unknown", this.execute(sender, "scheduler"));
        assertReducesTo("Tuff scheduler: leases", this.execute(sender, "scheduler", "leases"));
        assertReducesTo("Tuff scheduler: leases", this.execute(sender, "scheduler", "leases", "3"));
        assertReducesTo("Tuff scheduler: budget", this.execute(sender, "scheduler", "budget"));
        assertReducesTo("Tuff scheduler commands:", this.execute(sender, "scheduler", "help"));
        assertReducesTo("The Tuff thread layer has not been initialized yet", this.execute(sender, "threads"));
        assertNoDenial(sender);
    }

    @Test
    void aSchedulerOnlySenderIsRefusedEverythingElse() {
        final FakeSender sender = sender(TuffCommand.SCHEDULER_PERMISSION);

        assertNeedsControl("/tuff compute", this.execute(sender));
        assertNeedsControl("/tuff compute", this.execute(sender, "compute", "consumers"));
        assertNeedsControl("/tuff compute", this.execute(sender, "compute", "tasks"));
        assertNeedsControl("/tuff reload", this.execute(sender, "reload"));
        assertNeedsControl("/tuff reload", this.execute(sender, "compute", "reload"));
        assertNeedsControl("/tuff compute jfr start", this.execute(sender, "compute", "jfr", "start"));
        assertNeedsControl("/tuff compute jfr stop", this.execute(sender, "compute", "jfr", "stop"));
        assertNeedsControl("/tuff compute jfr dump", this.execute(sender, "compute", "jfr", "dump"));
        assertNeedsControl("/tuff scheduler reset", this.execute(sender, "scheduler", "reset"));
    }

    @Test
    void aRefusalNamesTheControlPermissionAndDoesNotRunTheAction() {
        final FakeSender sender = sender(TuffCommand.SCHEDULER_PERMISSION);

        final String reload = this.execute(sender, "reload");
        assertTrue(reload.contains(TuffCommand.PERMISSION), reload);
        assertFalse(reload.contains("Reloading the compute configuration"), reload);

        final String reset = this.execute(sender, "scheduler", "reset");
        assertTrue(reset.contains(TuffCommand.SCHEDULER_PERMISSION), reset);
        assertFalse(reset.contains("counters reset"), reset);
        assertFalse(reset.contains("counters not reset"), reset);
    }

    @Test
    void theControlPermissionReachesEverything() {
        final FakeSender sender = sender(TuffCommand.PERMISSION);

        assertFalse(this.execute(sender, "reload").contains("You do not have permission"));
        assertFalse(this.execute(sender, "compute", "jfr", "start").contains("You do not have permission"));
        assertFalse(this.execute(sender, "compute", "jfr", "stop").contains("You do not have permission"));
        assertFalse(this.execute(sender, "compute", "jfr", "dump").contains("You do not have permission"));
        assertReducesTo("Tuff scheduler: counters not reset", this.execute(sender, "scheduler", "reset"));
        assertReducesTo("The Tuff compute engine has not been initialized yet", this.execute(sender));
        assertReducesTo("Tuff scheduler: leases", this.execute(sender, "scheduler", "leases"));
        assertReducesTo("Tuff scheduler: budget", this.execute(sender, "scheduler", "budget"));
        assertReducesTo("Tuff scheduler commands:", this.execute(sender, "scheduler", "help"));
        assertReducesTo("The Tuff thread layer has not been initialized yet", this.execute(sender, "threads"));
        assertNoDenial(sender);
    }

    @Test
    void aSenderWithoutEitherPermissionIsDenied() {
        final FakeSender sender = sender();

        final String answer = this.execute(sender, "scheduler", "leases");

        assertTrue(answer.contains("You do not have permission to use the Tuff command."), answer);
        assertTrue(this.command.tabComplete(sender, "tuff", new String[] {""}).isEmpty(), "no completion");
        assertTrue(this.command.tabComplete(sender, "tuff", new String[] {"scheduler", ""}).isEmpty(),
            "no completion");
    }

    @Test
    void tabCompletionOnlyOffersTheReadOnlyGrammar() {
        final FakeSender sender = sender(TuffCommand.SCHEDULER_PERMISSION);

        assertEquals(List.of("threads", "scheduler"), this.complete(sender, ""));
        assertEquals(List.of("threads"), this.complete(sender, "t"));
        assertEquals(List.of("scheduler"), this.complete(sender, "sch"));
        assertEquals(List.of("leases", "budget", "help"), this.complete(sender, "scheduler", ""));
        assertEquals(List.of("budget"), this.complete(sender, "scheduler", "b"));
        assertEquals(List.of(), this.complete(sender, "scheduler", "reset", ""));
        assertEquals(List.of(), this.complete(sender, "compute", ""));
        assertEquals(List.of(), this.complete(sender, "compute", "jfr", "s"));
        assertEquals(List.of(), this.complete(sender, "reload", ""));
    }

    @Test
    void tabCompletionOfTheControlPermissionIsTheWholeGrammar() {
        final FakeSender sender = sender(TuffCommand.PERMISSION);

        assertEquals(List.of("compute", "status", "reload", "threads", "scheduler"), this.complete(sender, ""));
        assertEquals(List.of("consumers", "tasks", "reload", "jfr"), this.complete(sender, "compute", ""));
        assertEquals(List.of("start", "stop"), this.complete(sender, "compute", "jfr", "s"));
        assertEquals(List.of("leases", "budget", "reset", "help"), this.complete(sender, "scheduler", ""));
    }

    @Test
    void bothPermissionsTogetherBehaveLikeTheControlOne() {
        final FakeSender sender = sender(TuffCommand.PERMISSION, TuffCommand.SCHEDULER_PERMISSION);

        assertEquals(List.of("compute", "status", "reload", "threads", "scheduler"), this.complete(sender, ""));
        assertReducesTo("Tuff scheduler: counters not reset", this.execute(sender, "scheduler", "reset"));
    }

    private String execute(final FakeSender sender, final String... args) {
        sender.clear();
        assertTrue(this.command.execute(sender, "tuff", args), "execute always answers");
        final String text = sender.text();
        assertFalse(text.isBlank(), () -> "no answer for " + List.of(args));
        return text;
    }

    private List<String> complete(final FakeSender sender, final String... args) {
        return this.command.tabComplete(sender, "tuff", args);
    }

    private static void assertReducesTo(final String expected, final String answer) {
        assertTrue(answer.contains(expected), () -> "expected '" + expected + "' in: " + answer);
    }

    private static void assertNoDenial(final FakeSender sender) {
        assertFalse(sender.text().contains("You do not have permission"), sender.text());
    }

    private static void assertNeedsControl(final String form, final String answer) {
        assertTrue(answer.contains("You do not have permission to run " + form), answer);
        assertTrue(answer.contains(TuffCommand.PERMISSION), answer);
    }

    private static FakeSender sender(final String... permissions) {
        return new FakeSender(permissions);
    }

    /**
     * A sender that only knows a fixed set of permissions and keeps what it was told.
     */
    private static final class FakeSender implements CommandSender {

        private final Set<String> permissions;
        private final List<String> messages = new ArrayList<>(4);

        private FakeSender(final String... permissions) {
            this.permissions = Set.of(permissions);
        }

        private void clear() {
            this.messages.clear();
        }

        private String text() {
            return String.join("\n", this.messages);
        }

        @Override
        public void sendMessage(final String message) {
            this.messages.add(message);
        }

        @Override
        public void sendMessage(final String... messages) {
            this.messages.addAll(List.of(messages));
        }

        @Override
        public void sendMessage(final @Nullable UUID sender, final String message) {
            this.messages.add(message);
        }

        @Override
        public void sendMessage(final @Nullable UUID sender, final String... messages) {
            this.messages.addAll(List.of(messages));
        }

        @Override
        public Server getServer() {
            throw new UnsupportedOperationException("a permission test has no server");
        }

        @Override
        public String getName() {
            return "fake";
        }

        @Override
        public Spigot spigot() {
            throw new UnsupportedOperationException("a permission test never uses the spigot view");
        }

        @Override
        public Component name() {
            return Component.text("fake");
        }

        @Override
        public boolean isPermissionSet(final String name) {
            return this.permissions.contains(name);
        }

        @Override
        public boolean isPermissionSet(final Permission perm) {
            return this.isPermissionSet(perm.getName());
        }

        @Override
        public boolean hasPermission(final String name) {
            return this.permissions.contains(name);
        }

        @Override
        public boolean hasPermission(final Permission perm) {
            return this.hasPermission(perm.getName());
        }

        @Override
        public PermissionAttachment addAttachment(
            final Plugin plugin,
            final String name,
            final boolean value
        ) {
            throw new UnsupportedOperationException("a permission test never attaches anything");
        }

        @Override
        public PermissionAttachment addAttachment(final Plugin plugin) {
            throw new UnsupportedOperationException("a permission test never attaches anything");
        }

        @Override
        public @Nullable PermissionAttachment addAttachment(
            final Plugin plugin,
            final String name,
            final boolean value,
            final int ticks
        ) {
            throw new UnsupportedOperationException("a permission test never attaches anything");
        }

        @Override
        public @Nullable PermissionAttachment addAttachment(final Plugin plugin, final int ticks) {
            throw new UnsupportedOperationException("a permission test never attaches anything");
        }

        @Override
        public void removeAttachment(final PermissionAttachment attachment) {
            throw new UnsupportedOperationException("a permission test never attaches anything");
        }

        @Override
        public void recalculatePermissions() {
            throw new UnsupportedOperationException("the permissions of a fake sender are fixed");
        }

        @Override
        public Set<PermissionAttachmentInfo> getEffectivePermissions() {
            return Set.of();
        }

        @Override
        public boolean isOp() {
            return false;
        }

        @Override
        public void setOp(final boolean value) {
            throw new UnsupportedOperationException("a fake sender is not an operator");
        }
    }
}
