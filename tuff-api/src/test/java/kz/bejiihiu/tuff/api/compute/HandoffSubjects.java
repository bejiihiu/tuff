package kz.bejiihiu.tuff.api.compute;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.bukkit.World;
import org.bukkit.entity.Entity;
import org.bukkit.plugin.Plugin;

/**
 * Identity handles for the Bukkit types {@link ComputeHandoff} only passes through.
 *
 * <p>The delivery seam never calls the world or the entity, and only asks the plugin for its name
 * and logger, so a proxy is enough and no server, no Mockito and no half-implemented interfaces
 * are involved. Tests that need the real behaviour of a type must not use these.
 */
final class HandoffSubjects {

    private HandoffSubjects() {
    }

    /**
     * Creates a plugin handle that answers {@code getName} and {@code getLogger}.
     *
     * @return the plugin double
     */
    static Plugin plugin() {
        return plugin(capturingLogger(new CopyOnWriteArrayList<>()));
    }

    /**
     * Creates a plugin handle that logs into the given logger.
     *
     * @param logger logger returned by {@code getLogger}
     * @return the plugin double
     */
    static Plugin plugin(final Logger logger) {
        return proxy(Plugin.class, (method, arguments) -> switch (method.getName()) {
            case "getName" -> "handoff-test";
            case "getLogger" -> logger;
            case "isEnabled" -> true;
            case "toString" -> "HandoffTestPlugin";
            default -> throw new UnsupportedOperationException(method.getName());
        });
    }

    /**
     * Creates a world handle. The seam never reads anything from it.
     *
     * @return the world double
     */
    static World world() {
        return proxy(World.class, (method, arguments) -> switch (method.getName()) {
            case "getName" -> "handoff-test-world";
            case "toString" -> "HandoffTestWorld";
            default -> throw new UnsupportedOperationException(method.getName());
        });
    }

    /**
     * Creates an entity handle. The seam never reads anything from it.
     *
     * @return the entity double
     */
    static Entity entity() {
        return proxy(Entity.class, (method, arguments) -> switch (method.getName()) {
            case "toString" -> "HandoffTestEntity";
            default -> throw new UnsupportedOperationException(method.getName());
        });
    }

    /**
     * Creates a logger that records everything at {@link Level#ALL} and does not print.
     *
     * @param records sink for the captured records
     * @return the recording logger
     */
    static Logger capturingLogger(final List<LogRecord> records) {
        final Logger logger = Logger.getLogger("handoff-test-" + System.identityHashCode(records));
        logger.setUseParentHandlers(false);
        logger.setLevel(Level.ALL);
        logger.addHandler(new Handler() {
            @Override
            public void publish(final LogRecord record) {
                records.add(record);
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        });
        return logger;
    }

    private static <T> T proxy(final Class<T> type, final BiFunction<Method, Object[], Object> handler) {
        final Object instance = Proxy.newProxyInstance(
            type.getClassLoader(),
            new Class<?>[] {type},
            (proxy, method, arguments) -> switch (method.getName()) {
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == arguments[0];
                default -> handler.apply(method, arguments == null ? new Object[0] : arguments);
            }
        );
        return type.cast(instance);
    }
}
