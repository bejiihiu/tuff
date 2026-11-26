package kz.bejiihiu.tuff.compute.internal.spi;

import java.util.concurrent.ThreadFactory;
import kz.bejiihiu.tuff.compute.internal.util.ThreadFactories;

/**
 * Default {@link ComputeThreadFactoryPort}: delegates to {@link ThreadFactories} unchanged.
 *
 * <p>No wrapper, no renamed thread, no extra handler. That is what makes a scheduler built without a
 * manager behave byte for byte like the one built before the port existed.
 */
final class LegacyComputeThreadFactoryPort implements ComputeThreadFactoryPort {

    static final LegacyComputeThreadFactoryPort INSTANCE = new LegacyComputeThreadFactoryPort();

    private LegacyComputeThreadFactoryPort() {
    }

    @Override
    public ThreadFactory platform(final String namePrefix, final boolean daemon) {
        return ThreadFactories.platform(namePrefix, daemon);
    }

    @Override
    public ThreadFactory virtual(final String namePrefix) {
        return ThreadFactories.virtual(namePrefix);
    }

    @Override
    public ThreadFactory named(final String name, final boolean daemon) {
        return ThreadFactories.named(name, daemon);
    }
}
