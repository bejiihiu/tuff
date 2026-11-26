package kz.bejiihiu.tuff.compute.internal.spi;

import java.util.concurrent.ThreadFactory;

/**
 * The single source of threads for the compute engine.
 *
 * <p>Everything that starts a thread - platform workers, the completion executor, the metrics
 * sampler, the janitor, the shutdown thread, the adaptive timer, the virtual lane and its pump -
 * asks this port. A manager that wants to register or wrap compute threads implements it once and
 * keeps its own bookkeeping; the engine only ever sees a {@link ThreadFactory}.
 *
 * <p>An implementation has to preserve what the engine relies on: platform workers must be real
 * platform threads (the pool rejects virtual ones), and names matter operationally because thread
 * dumps and the shutdown report are searched by the worker prefix. Returning a factory that renames
 * threads is allowed, but then that name becomes the new operational contract.
 */
public interface ComputeThreadFactoryPort {

    /**
     * Returns a factory for platform threads.
     *
     * <p>Names follow {@code <namePrefix>-<number>} with a counter starting at one, exactly like the
     * legacy factory, so a thread dump still shows which pool a thread belongs to.
     *
     * @param namePrefix prefix for thread names, must not be blank
     * @param daemon     whether created threads are daemons; workers are daemons on purpose
     * @return the factory
     */
    ThreadFactory platform(String namePrefix, boolean daemon);

    /**
     * Returns a factory for virtual threads.
     *
     * <p>Used for the completion executor and for the bounded virtual lane, never for platform
     * workers.
     *
     * @param namePrefix prefix for thread names, must not be blank
     * @return the factory
     */
    ThreadFactory virtual(String namePrefix);

    /**
     * Returns a factory whose thread carries exactly {@code name}.
     *
     * <p>For the one-off service threads (janitor, shutdown, lane pump) whose names are already
     * unique and are part of the diagnostics contract. The factory is meant to be used for a single
     * thread; two threads from one instance would share a name.
     *
     * @param name   exact thread name, must not be blank
     * @param daemon whether the created thread is a daemon
     * @return the factory
     */
    ThreadFactory named(String name, boolean daemon);

    /**
     * Returns the default port: the legacy factories of this module.
     *
     * @return the legacy port, never {@code null}
     */
    static ComputeThreadFactoryPort legacy() {
        return LegacyComputeThreadFactoryPort.INSTANCE;
    }
}
