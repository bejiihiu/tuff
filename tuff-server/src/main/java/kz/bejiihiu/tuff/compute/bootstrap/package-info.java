/**
 * Server lifecycle integration of the compute engine.
 *
 * <p>{@link kz.bejiihiu.tuff.compute.bootstrap.ComputeBootstrap} owns the engine: it reads
 * {@code config/tuff.yml} while the server is being built, starts the scheduler, publishes it through
 * {@link kz.bejiihiu.tuff.api.compute.TuffCompute}, re-sizes the pool once the region scheduler is up
 * and therefore knows how many threads the rest of the server owns, and stops everything during
 * shutdown. The {@code /tuff} command lives here as well, next to the pure argument parsing and
 * report formatting it uses.
 *
 * <p>Nothing in this package may let an exception escape a lifecycle hook. A broken configuration or a
 * failing worker pool is logged and shown by the command; it never prevents the server from starting
 * or from stopping.
 */
@NullMarked
package kz.bejiihiu.tuff.compute.bootstrap;

import org.jspecify.annotations.NullMarked;
