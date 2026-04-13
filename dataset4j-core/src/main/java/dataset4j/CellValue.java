package dataset4j;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Objects;

/**
 * An immutable value container that preserves source type information
 * and format metadata from the originating data source (Excel, Parquet, CSV, etc.).
 *
 * <p>Each cell in a {@link Table} is represented as a {@code CellValue} that carries:
 * <ul>
 *   <li>{@code value} &mdash; the raw Java value (may be {@code null} for {@link ValueType#BLANK})</li>
 *   <li>{@code type} &mdash; a normalized {@link ValueType} describing the source type</li>
 *   <li>{@code format} &mdash; an optional format string from the source (e.g. Excel {@code "$#,##0.00"},
 *       {@code "yyyy-MM-dd"}), or {@code null} if no format applies</li>
 * </ul>
 *
 * <p>Typed accessors ({@link #asInt()}, {@link #asDouble()}, {@link #asString()}, etc.)
 * perform <em>coercion</em> rather than simple casting:
 * <ul>
 *   <li>A {@code Double} value coerces to {@code int} via {@code intValue()}</li>
 *   <li>A {@code String} value {@code "123"} coerces to {@code int} via parsing</li>
 *   <li>A {@code Number} coerces to {@code String} via {@code toString()}</li>
 * </ul>
 *
 * @param value  the raw Java value ({@code null} for blank cells)
 * @param type   the normalized source type
 * @param format the source format string, or {@code null}
 */
public record CellValue(Object value, ValueType type, String format) {

    // ---------------------------------------------------------------
    // Factories
    // ---------------------------------------------------------------

    /** Create a CellValue with no format. */
    public static CellValue of(Object value, ValueType type) {
        return new CellValue(value, type, null);
    }

    /** Create a CellValue with format. */
    public static CellValue of(Object value, ValueType type, String format) {
        return new CellValue(value, type, format);
    }

    /** Create a STRING CellValue. */
    public static CellValue ofString(String value) {
        return new CellValue(value, ValueType.STRING, null);
    }

    /** Create a NUMBER CellValue. */
    public static CellValue ofNumber(Number value) {
        return new CellValue(value, ValueType.NUMBER, null);
    }

    /** Create a NUMBER CellValue with format. */
    public static CellValue ofNumber(Number value, String format) {
        return new CellValue(value, ValueType.NUMBER, format);
    }

    /** Create a BOOLEAN CellValue. */
    public static CellValue ofBoolean(Boolean value) {
        return new CellValue(value, ValueType.BOOLEAN, null);
    }

    /** Create a DATE CellValue. */
    public static CellValue ofDate(LocalDate value) {
        return new CellValue(value, ValueType.DATE, null);
    }

    /** Create a DATE CellValue with format. */
    public static CellValue ofDate(LocalDate value, String format) {
        return new CellValue(value, ValueType.DATE, format);
    }

    /** Create a DATETIME CellValue. */
    public static CellValue ofDateTime(LocalDateTime value) {
        return new CellValue(value, ValueType.DATETIME, null);
    }

    /** Create a DATETIME CellValue with format. */
    public static CellValue ofDateTime(LocalDateTime value, String format) {
        return new CellValue(value, ValueType.DATETIME, format);
    }

    /** Create a TIME CellValue. */
    public static CellValue ofTime(LocalTime value) {
        return new CellValue(value, ValueType.TIME, null);
    }

    /** Create a BLANK CellValue. */
    public static CellValue blank() {
        return new CellValue(null, ValueType.BLANK, null);
    }

    /** Create an ERROR CellValue. */
    public static CellValue error(String message) {
        return new CellValue(message, ValueType.ERROR, null);
    }

    /** Create a FORMULA CellValue. */
    public static CellValue ofFormula(String expression) {
        return new CellValue(expression, ValueType.FORMULA, null);
    }

    // ---------------------------------------------------------------
    // Typed accessors with coercion
    // ---------------------------------------------------------------

    /**
     * Coerce the value to {@code String}.
     * @return the string representation, or {@code null} if blank
     */
    public String asString() {
        if (value == null) return null;
        return value.toString();
    }

    /**
     * Coerce the value to {@code int}.
     *
     * <ul>
     *   <li>{@code Number} &rarr; {@code intValue()}</li>
     *   <li>{@code String} &rarr; parsed as integer (strips decimals if needed)</li>
     *   <li>{@code Boolean} &rarr; {@code 1} / {@code 0}</li>
     * </ul>
     *
     * @return the coerced int value
     * @throws CellCoercionException if the value cannot be coerced
     */
    public int asInt() {
        if (value == null) throw coercionError("int");
        if (value instanceof Number n) return n.intValue();
        if (value instanceof Boolean b) return b ? 1 : 0;
        if (value instanceof String s) {
            try {
                // Handle "3.0" → 3
                if (s.contains(".")) {
                    return (int) Double.parseDouble(s.trim());
                }
                return Integer.parseInt(s.trim());
            } catch (NumberFormatException e) {
                throw coercionError("int", e);
            }
        }
        throw coercionError("int");
    }

    /**
     * Coerce the value to {@code long}.
     *
     * @return the coerced long value
     * @throws CellCoercionException if the value cannot be coerced
     */
    public long asLong() {
        if (value == null) throw coercionError("long");
        if (value instanceof Number n) return n.longValue();
        if (value instanceof Boolean b) return b ? 1L : 0L;
        if (value instanceof String s) {
            try {
                if (s.contains(".")) {
                    return (long) Double.parseDouble(s.trim());
                }
                return Long.parseLong(s.trim());
            } catch (NumberFormatException e) {
                throw coercionError("long", e);
            }
        }
        throw coercionError("long");
    }

    /**
     * Coerce the value to {@code double}.
     *
     * @return the coerced double value
     * @throws CellCoercionException if the value cannot be coerced
     */
    public double asDouble() {
        if (value == null) throw coercionError("double");
        if (value instanceof Number n) return n.doubleValue();
        if (value instanceof Boolean b) return b ? 1.0 : 0.0;
        if (value instanceof String s) {
            try {
                return Double.parseDouble(s.trim());
            } catch (NumberFormatException e) {
                throw coercionError("double", e);
            }
        }
        throw coercionError("double");
    }

    /**
     * Coerce the value to {@code BigDecimal}.
     *
     * @return the coerced BigDecimal value
     * @throws CellCoercionException if the value cannot be coerced
     */
    public BigDecimal asBigDecimal() {
        if (value == null) throw coercionError("BigDecimal");
        if (value instanceof BigDecimal bd) return bd;
        if (value instanceof Number n) return BigDecimal.valueOf(n.doubleValue());
        if (value instanceof String s) {
            try {
                return new BigDecimal(s.trim());
            } catch (NumberFormatException e) {
                throw coercionError("BigDecimal", e);
            }
        }
        throw coercionError("BigDecimal");
    }

    /**
     * Coerce the value to {@code boolean}.
     *
     * <ul>
     *   <li>{@code Boolean} &rarr; direct</li>
     *   <li>{@code Number} &rarr; non-zero is {@code true}</li>
     *   <li>{@code String} &rarr; "true"/"1"/"yes" (case-insensitive)</li>
     * </ul>
     *
     * @return the coerced boolean value
     * @throws CellCoercionException if the value cannot be coerced
     */
    public boolean asBoolean() {
        if (value == null) throw coercionError("boolean");
        if (value instanceof Boolean b) return b;
        if (value instanceof Number n) return n.doubleValue() != 0.0;
        if (value instanceof String s) {
            String lower = s.trim().toLowerCase();
            if ("true".equals(lower) || "1".equals(lower) || "yes".equals(lower)) return true;
            if ("false".equals(lower) || "0".equals(lower) || "no".equals(lower)) return false;
            throw coercionError("boolean");
        }
        throw coercionError("boolean");
    }

    /**
     * Coerce the value to {@code LocalDate}.
     *
     * @return the coerced LocalDate value
     * @throws CellCoercionException if the value cannot be coerced
     */
    public LocalDate asLocalDate() {
        if (value == null) throw coercionError("LocalDate");
        if (value instanceof LocalDate ld) return ld;
        if (value instanceof LocalDateTime ldt) return ldt.toLocalDate();
        if (value instanceof String s) {
            try {
                return LocalDate.parse(s.trim());
            } catch (Exception e) {
                throw coercionError("LocalDate", e);
            }
        }
        throw coercionError("LocalDate");
    }

    /**
     * Coerce the value to {@code LocalDateTime}.
     *
     * @return the coerced LocalDateTime value
     * @throws CellCoercionException if the value cannot be coerced
     */
    public LocalDateTime asLocalDateTime() {
        if (value == null) throw coercionError("LocalDateTime");
        if (value instanceof LocalDateTime ldt) return ldt;
        if (value instanceof LocalDate ld) return ld.atStartOfDay();
        if (value instanceof String s) {
            try {
                return LocalDateTime.parse(s.trim());
            } catch (Exception e) {
                throw coercionError("LocalDateTime", e);
            }
        }
        throw coercionError("LocalDateTime");
    }

    /**
     * Coerce the value to {@code LocalTime}.
     *
     * @return the coerced LocalTime value
     * @throws CellCoercionException if the value cannot be coerced
     */
    public LocalTime asLocalTime() {
        if (value == null) throw coercionError("LocalTime");
        if (value instanceof LocalTime lt) return lt;
        if (value instanceof LocalDateTime ldt) return ldt.toLocalTime();
        if (value instanceof String s) {
            try {
                return LocalTime.parse(s.trim());
            } catch (Exception e) {
                throw coercionError("LocalTime", e);
            }
        }
        throw coercionError("LocalTime");
    }

    // ---------------------------------------------------------------
    // Query helpers
    // ---------------------------------------------------------------

    /** Whether the value is null or BLANK. */
    public boolean isBlank() {
        return value == null || type == ValueType.BLANK;
    }

    /** Whether the value has a format string. */
    public boolean hasFormat() {
        return format != null && !format.isEmpty();
    }

    // ---------------------------------------------------------------
    // Error helpers
    // ---------------------------------------------------------------

    private CellCoercionException coercionError(String targetType) {
        return new CellCoercionException(value, type, targetType);
    }

    private CellCoercionException coercionError(String targetType, Throwable cause) {
        return new CellCoercionException(value, type, targetType, cause);
    }

    @Override
    public String toString() {
        if (value == null) return "CellValue[BLANK]";
        return "CellValue[" + type + ": " + value + (format != null ? " (" + format + ")" : "") + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CellValue that)) return false;
        return type == that.type && Objects.equals(value, that.value) && Objects.equals(format, that.format);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, type, format);
    }
}
