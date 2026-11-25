/**
 * Default adaptive concurrency controller.
 *
 * <p>Contains the AIMD controller the engine instantiates and the flight recorder event that
 * explains its decisions. Nothing in here is part of the public scheduler API; the engine is the only
 * expected caller of the controller, while the event is read from a recording.
 */
@NullMarked
package kz.bejiihiu.tuff.compute.internal.adaptive.impl;

import org.jspecify.annotations.NullMarked;
