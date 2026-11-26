/**
 * Tests that the observe-only thread layer is invisible to everything else.
 *
 * <p>The package holds the cross-cutting checks: the compute engine keeps its own threads and budget,
 * and the server files the project patches keep exactly the hooks they already had. The lifecycle and
 * the config live next to their own code.
 */
@NullMarked
package kz.bejiihiu.tuff.threads.observe;

import org.jspecify.annotations.NullMarked;
