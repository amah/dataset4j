package dataset4j;

/**
 * Normalized type classifier for untyped tabular data.
 *
 * <p>This is a superset of types that can originate from Excel, Parquet, CSV,
 * or other tabular sources. Each {@link CellValue} carries a {@code ValueType}
 * that describes the <em>source</em> type of the data, enabling smart coercion
 * and round-trip fidelity when converting between formats.
 *
 * <table>
 *   <caption>Source type mapping</caption>
 *   <tr><th>Source</th><th>Mapping</th></tr>
 *   <tr><td>Excel</td><td>POI CellType &rarr; ValueType, format string preserved</td></tr>
 *   <tr><td>Parquet</td><td>Physical/logical type &rarr; ValueType</td></tr>
 *   <tr><td>CSV</td><td>All values are {@link #STRING} with null format</td></tr>
 * </table>
 */
public enum ValueType {

    /** Text / string values. */
    STRING,

    /** Numeric values (integer, floating-point, decimal). */
    NUMBER,

    /** Boolean true/false values. */
    BOOLEAN,

    /** Date-only values (e.g. {@code LocalDate}). */
    DATE,

    /** Date+time values (e.g. {@code LocalDateTime}). */
    DATETIME,

    /** Time-only values (e.g. {@code LocalTime}). */
    TIME,

    /** Excel formula expression (e.g. {@code =SUM(A1:A10)}). */
    FORMULA,

    /** Blank / empty cell. */
    BLANK,

    /** Error value (e.g. Excel {@code #REF!}, {@code #DIV/0!}). */
    ERROR
}
