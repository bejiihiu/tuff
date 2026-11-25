/**
 * Configuration of the compute scheduler.
 *
 * <p>This package owns {@code config/tuff.yml}: it reads the file as UTF-8, validates every value
 * against {@link kz.bejiihiu.tuff.compute.ComputeSettings}, publishes an immutable snapshot and hands
 * changes to an optional apply hook. Wrong values, unknown keys and unreadable files are reported and
 * replaced by defaults; nothing here is allowed to stop the server.
 */
@NullMarked
package kz.bejiihiu.tuff.compute.config;

import org.jspecify.annotations.NullMarked;
