/**
 * Small shared helpers: the scheduler clock, argument checks, throwable handling and thread
 * factories.
 *
 * <p>These exist so that eight components do not each invent their own {@code requireNonNull}
 * wrapper, their own thread naming scheme or their own sneaky rethrow. Everything here is stateless
 * and thread safe.
 */
@NullMarked
package kz.bejiihiu.tuff.compute.internal.util;

import org.jspecify.annotations.NullMarked;
