package kz.bejiihiu.tuff.threads.internal.spi;

import kz.bejiihiu.tuff.threads.ThreadManagementSettings;

/**
 * Single creation seam of the thread manager.
 *
 * <p>The factory exists so bootstrap does not have to know the implementation class, and so tests can
 * build a runtime with an injected clock, topology and sink. Exactly one factory instance is installed by
 * the server bootstrap; plugins never see this interface.
 *
 * <p>Creating a runtime is side effect free: no thread is created, no pool is registered and no file is
 * touched. All of that happens in {@link ThreadManagerRuntime#start()}. A factory created with a
 * disabled configuration still returns a usable runtime in the NEW state; it just refuses work once
 * started.
 */
public interface ThreadManagerFactory {

    /**
     * Creates a runtime for a configuration.
     *
     * <p>Validation is already done by {@link ThreadManagementSettings}; the factory only assembles the
     * implementation. The returned runtime is in its initial state and has to be started by the caller.
     *
     * @param settings effective configuration
     * @return a runtime that is not started yet
     * @throws NullPointerException if {@code settings} is {@code null}
     */
    ThreadManagerRuntime create(ThreadManagementSettings settings);
}
