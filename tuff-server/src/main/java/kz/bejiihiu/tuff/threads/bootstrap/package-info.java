/**
 * Server lifecycle integration of the unified thread layer.
 *
 * <p>{@link kz.bejiihiu.tuff.threads.bootstrap.ThreadsBootstrap} is called from the existing compute
 * lifecycle hooks, so the server needs no new patch: the thread layer starts before the compute engine
 * and stops after it, which is the order both subsystems require. The {@code /tuff threads} command lives
 * next to the pure report formatter it uses.
 *
 * <p>The default is observation only. The runtime registers the canonical pools, Folia keeps its
 * scheduler, and the region pool is read through a probe. A disabled {@code thread-management} section
 * creates no runtime and no thread at all.
 *
 * <p>Nothing in this package may let an exception escape a lifecycle hook. A broken configuration or a
 * failed runtime is logged and shown by the command; it never prevents the server from starting or
 * stopping.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.bootstrap;

import org.jspecify.annotations.NullMarked;
