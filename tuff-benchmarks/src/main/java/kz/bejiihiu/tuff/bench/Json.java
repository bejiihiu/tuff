package kz.bejiihiu.tuff.bench;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal JSON reader and writer, so the harness depends on the JDK only.
 *
 * <p>The artifact schema is small and owned by this package: objects, arrays, strings, numbers,
 * booleans and null. Parsing produces {@link Map}, {@link List}, {@link String}, {@link Long},
 * {@link Double}, {@link Boolean} or {@code null}; writing accepts the same shapes. No escaping rules
 * beyond RFC 8259 are reinterpreted, and a malformed document is a hard error instead of a guess.
 */
final class Json {

    private Json() {
    }

    /**
     * Serialises a value as pretty printed JSON with a trailing newline.
     *
     * @param value maps, lists, strings, numbers, booleans or {@code null}
     * @return the document
     * @throws IllegalArgumentException if a value has no JSON representation
     */
    static String stringify(final Object value) {
        final StringBuilder out = new StringBuilder(1024);
        write(out, value, 0);
        out.append('\n');
        return out.toString();
    }

    /**
     * Writes a value as pretty printed JSON.
     *
     * @param path  destination
     * @param value value to write
     * @throws IOException if the file cannot be written
     */
    static void write(final Path path, final Object value) throws IOException {
        final Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.writeString(path, stringify(value), StandardCharsets.UTF_8);
    }

    /**
     * Reads a JSON document.
     *
     * @param path source file
     * @return the parsed value
     * @throws IOException              if the file cannot be read
     * @throws IllegalArgumentException if the document is malformed
     */
    static Object read(final Path path) throws IOException {
        return parse(Files.readString(path, StandardCharsets.UTF_8));
    }

    /**
     * Parses a JSON document.
     *
     * @param text document text
     * @return the parsed value
     * @throws IllegalArgumentException if the document is malformed
     */
    static Object parse(final String text) {
        final Parser parser = new Parser(text);
        final Object value = parser.value();
        parser.skipWhitespace();
        if (!parser.atEnd()) {
            throw new IllegalArgumentException("trailing characters at offset " + parser.offset);
        }
        return value;
    }

    private static void write(final StringBuilder out, final Object value, final int depth) {
        switch (value) {
            case null -> out.append("null");
            case String text -> writeString(out, text);
            case Boolean bool -> out.append(bool);
            case Double number -> writeNumber(out, number);
            case Float number -> writeNumber(out, number.doubleValue());
            case Number number -> out.append(number.longValue());
            case Map<?, ?> map -> writeObject(out, map, depth);
            case List<?> list -> writeArray(out, list, depth);
            case Object[] array -> writeArray(out, List.of(array), depth);
            default -> throw new IllegalArgumentException(
                "no JSON representation for " + value.getClass().getName()
            );
        }
    }

    private static void writeNumber(final StringBuilder out, final double number) {
        if (Double.isNaN(number) || Double.isInfinite(number)) {
            // json has no NaN; a measurement that could not be taken is null, not a fake zero
            out.append("null");
            return;
        }
        if (number == Math.rint(number) && Math.abs(number) < 9.007_199_254_740_992E15d) {
            out.append((long) number);
            return;
        }
        out.append(number);
    }

    private static void writeObject(final StringBuilder out, final Map<?, ?> map, final int depth) {
        if (map.isEmpty()) {
            out.append("{}");
            return;
        }
        out.append("{\n");
        int index = 0;
        for (final Map.Entry<?, ?> entry : map.entrySet()) {
            indent(out, depth + 1);
            writeString(out, String.valueOf(entry.getKey()));
            out.append(": ");
            write(out, entry.getValue(), depth + 1);
            if (++index < map.size()) {
                out.append(',');
            }
            out.append('\n');
        }
        indent(out, depth);
        out.append('}');
    }

    private static void writeArray(final StringBuilder out, final List<?> list, final int depth) {
        if (list.isEmpty()) {
            out.append("[]");
            return;
        }
        out.append("[\n");
        for (int index = 0; index < list.size(); ++index) {
            indent(out, depth + 1);
            write(out, list.get(index), depth + 1);
            if (index + 1 < list.size()) {
                out.append(',');
            }
            out.append('\n');
        }
        indent(out, depth);
        out.append(']');
    }

    private static void indent(final StringBuilder out, final int depth) {
        out.append("  ".repeat(depth));
    }

    private static void writeString(final StringBuilder out, final String text) {
        out.append('"');
        for (int index = 0; index < text.length(); ++index) {
            final char symbol = text.charAt(index);
            switch (symbol) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (symbol < 0x20) {
                        out.append(String.format("\\u%04x", (int) symbol));
                    } else {
                        out.append(symbol);
                    }
                }
            }
        }
        out.append('"');
    }

    private static final class Parser {

        private final String text;
        private int offset;

        private Parser(final String text) {
            this.text = text;
        }

        private boolean atEnd() {
            return this.offset >= this.text.length();
        }

        private void skipWhitespace() {
            while (!this.atEnd()) {
                final char symbol = this.text.charAt(this.offset);
                if (symbol == ' ' || symbol == '\t' || symbol == '\n' || symbol == '\r') {
                    ++this.offset;
                } else {
                    return;
                }
            }
        }

        private char peek() {
            if (this.atEnd()) {
                throw new IllegalArgumentException("unexpected end of document");
            }
            return this.text.charAt(this.offset);
        }

        private void expect(final char expected) {
            if (this.atEnd() || this.text.charAt(this.offset) != expected) {
                throw new IllegalArgumentException(
                    "expected '" + expected + "' at offset " + this.offset
                );
            }
            ++this.offset;
        }

        private Object value() {
            this.skipWhitespace();
            final char symbol = this.peek();
            return switch (symbol) {
                case '{' -> this.object();
                case '[' -> this.array();
                case '"' -> this.string();
                case 't' -> this.literal("true", Boolean.TRUE);
                case 'f' -> this.literal("false", Boolean.FALSE);
                case 'n' -> this.literal("null", null);
                default -> this.number();
            };
        }

        private Object literal(final String literal, final Object value) {
            if (!this.text.startsWith(literal, this.offset)) {
                throw new IllegalArgumentException("invalid literal at offset " + this.offset);
            }
            this.offset += literal.length();
            return value;
        }

        private Map<String, Object> object() {
            this.expect('{');
            final Map<String, Object> map = new LinkedHashMap<>();
            this.skipWhitespace();
            if (!this.atEnd() && this.peek() == '}') {
                ++this.offset;
                return map;
            }
            while (true) {
                this.skipWhitespace();
                final String key = this.string();
                this.skipWhitespace();
                this.expect(':');
                map.put(key, this.value());
                this.skipWhitespace();
                final char next = this.peek();
                if (next == ',') {
                    ++this.offset;
                    continue;
                }
                this.expect('}');
                return map;
            }
        }

        private List<Object> array() {
            this.expect('[');
            final List<Object> list = new ArrayList<>();
            this.skipWhitespace();
            if (!this.atEnd() && this.peek() == ']') {
                ++this.offset;
                return list;
            }
            while (true) {
                list.add(this.value());
                this.skipWhitespace();
                final char next = this.peek();
                if (next == ',') {
                    ++this.offset;
                    continue;
                }
                this.expect(']');
                return list;
            }
        }

        private String string() {
            this.expect('"');
            final StringBuilder out = new StringBuilder();
            while (true) {
                if (this.atEnd()) {
                    throw new IllegalArgumentException("unterminated string");
                }
                final char symbol = this.text.charAt(this.offset++);
                if (symbol == '"') {
                    return out.toString();
                }
                if (symbol != '\\') {
                    out.append(symbol);
                    continue;
                }
                if (this.atEnd()) {
                    throw new IllegalArgumentException("unterminated escape");
                }
                final char escaped = this.text.charAt(this.offset++);
                switch (escaped) {
                    case '"' -> out.append('"');
                    case '\\' -> out.append('\\');
                    case '/' -> out.append('/');
                    case 'b' -> out.append('\b');
                    case 'f' -> out.append('\f');
                    case 'n' -> out.append('\n');
                    case 'r' -> out.append('\r');
                    case 't' -> out.append('\t');
                    case 'u' -> {
                        if (this.offset + 4 > this.text.length()) {
                            throw new IllegalArgumentException("truncated unicode escape");
                        }
                        out.append((char) Integer.parseInt(this.text.substring(this.offset, this.offset + 4), 16));
                        this.offset += 4;
                    }
                    default -> throw new IllegalArgumentException("invalid escape \\" + escaped);
                }
            }
        }

        private Object number() {
            final int start = this.offset;
            if (!this.atEnd() && (this.peek() == '-' || this.peek() == '+')) {
                ++this.offset;
            }
            boolean fractional = false;
            while (!this.atEnd()) {
                final char symbol = this.text.charAt(this.offset);
                if (symbol >= '0' && symbol <= '9') {
                    ++this.offset;
                } else if (symbol == '.' || symbol == 'e' || symbol == 'E' || symbol == '+' || symbol == '-') {
                    fractional = true;
                    ++this.offset;
                } else {
                    break;
                }
            }
            if (start == this.offset) {
                throw new IllegalArgumentException("expected a value at offset " + start);
            }
            final String literal = this.text.substring(start, this.offset);
            try {
                return fractional ? (Object) Double.parseDouble(literal) : (Object) Long.parseLong(literal);
            } catch (final NumberFormatException failure) {
                throw new IllegalArgumentException("invalid number '" + literal + "'", failure);
            }
        }
    }

    /**
     * Reads a value out of a parsed object as a string.
     *
     * @param map  parsed object
     * @param key  field name
     * @param fall fallback used when the field is absent or null
     * @return the text value
     */
    static String string(final Map<String, Object> map, final String key, final String fall) {
        final Object value = map.get(key);
        return value == null ? fall : String.valueOf(value);
    }

    /**
     * Reads a numeric field, accepting both integer and fractional JSON numbers.
     *
     * @param map  parsed object
     * @param key  field name
     * @param fall fallback used when the field is absent or not a number
     * @return the number, or the fallback
     */
    static double number(final Map<String, Object> map, final String key, final double fall) {
        return map.get(key) instanceof final Number value ? value.doubleValue() : fall;
    }

    /**
     * Reads an integral numeric field.
     *
     * @param map  parsed object
     * @param key  field name
     * @param fall fallback used when the field is absent or not a number
     * @return the number, or the fallback
     */
    static long integer(final Map<String, Object> map, final String key, final long fall) {
        return map.get(key) instanceof final Number value ? value.longValue() : fall;
    }

    /**
     * Reads a boolean field.
     *
     * @param map  parsed object
     * @param key  field name
     * @param fall fallback used when the field is absent or not a boolean
     * @return the value, or the fallback
     */
    static boolean bool(final Map<String, Object> map, final String key, final boolean fall) {
        return map.get(key) instanceof final Boolean value ? value : fall;
    }
}
