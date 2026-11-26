/**
 * Tests for admission, leases and saturation reporting.
 *
 * <p>The interesting cases are the ones that must not hang: a blocked waiter during shutdown, a region
 * thread asking for a wait, and two waiters competing for one global permit. Every blocking test has a
 * hard join timeout so a deadlock shows up as a failed assertion instead of a stuck build.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.internal.impl.controller;

import org.jspecify.annotations.NullMarked;
