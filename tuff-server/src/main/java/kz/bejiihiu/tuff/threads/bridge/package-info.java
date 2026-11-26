/**
 * The bridge between Folia's region scheduler and the unified thread layer.
 *
 * <p>Region tick threads cannot be created elsewhere: their concrete class is checked with
 * {@code instanceof} on every tick, their group feeds NUMA collection and their uncaught handler is
 * the path that stops a broken server. The only integration that respects all of that is borrowing
 * accounting for a body somebody else runs, which is what this package exists for.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.bridge;

import org.jspecify.annotations.NullMarked;
