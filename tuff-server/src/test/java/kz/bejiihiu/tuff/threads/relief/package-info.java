/**
 * Tests of the per-region overload trigger: hysteresis, cooldown, rate limit, independence and unit
 * isolation, all driven through the scheduler independent tick feed with a controlled clock.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.relief;

import org.jspecify.annotations.NullMarked;
