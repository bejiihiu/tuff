/**
 * Tests of the compute engine and the public scheduler facade.
 *
 * <p>The tests drive the real queue, pool, metrics and adaptive implementations; only the clock is
 * replaced, because it is a documented extension point used to drive aging, expiry and the adaptive
 * window deterministically.
 */
@NullMarked
package kz.bejiihiu.tuff.compute.internal.engine.impl;

import org.jspecify.annotations.NullMarked;

