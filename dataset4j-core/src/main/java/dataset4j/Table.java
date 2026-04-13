package dataset4j;

import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.*;
import java.util.stream.Collectors;

/**
 * An immutable, row-oriented container for untyped tabular data.
 *
 * <p>{@code Table} is the schema-free sibling of {@link Dataset}: where {@code Dataset<T>}
 * requires a Java record class, {@code Table} stores rows as maps of column name to
 * {@link CellValue}, preserving source type information and format metadata.
 *
 * <p>All operations return new {@code Table} instances (immutable).
 *
 * <pre>
 * Table table = Table.builder()
 *     .columns("name", "age", "dept")
 *     .row("Alice", 30, "Eng")
 *     .row("Bob", 25, "Sales")
 *     .build();
 *
 * table.filter(row -&gt; row.get("age").asInt() &gt; 25)
 *      .select("name", "dept")
 *      .sortBy("name");
 * </pre>
 */
public class Table {

    private final List<String> columns;
    private final List<Map<String, CellValue>> rows;

    // ---------------------------------------------------------------
    // Construction
    // ---------------------------------------------------------------

    private Table(List<String> columns, List<Map<String, CellValue>> rows) {
        this.columns = List.copyOf(columns);
        this.rows = rows.stream()
                .map(Map::copyOf)
                .collect(Collectors.toUnmodifiableList());
    }

    /**
     * Create a Table from column names and rows of CellValues.
     */
    public static Table of(List<String> columns, List<Map<String, CellValue>> rows) {
        return new Table(columns, rows);
    }

    /**
     * Create an empty Table with the given column names.
     */
    public static Table empty(String... columns) {
        return new Table(List.of(columns), List.of());
    }

    /**
     * Create an empty Table with the given column names.
     */
    public static Table empty(List<String> columns) {
        return new Table(columns, List.of());
    }

    /**
     * Start building a Table with a fluent API.
     */
    public static Builder builder() {
        return new Builder();
    }

    // ---------------------------------------------------------------
    // Schema inspection
    // ---------------------------------------------------------------

    /** Ordered list of column names. */
    public List<String> columns() {
        return columns;
    }

    /** Number of columns. */
    public int columnCount() {
        return columns.size();
    }

    /** Number of rows. */
    public int size() {
        return rows.size();
    }

    /** Whether the table has zero rows. */
    public boolean isEmpty() {
        return rows.isEmpty();
    }

    /** Whether a column with the given name exists. */
    public boolean hasColumn(String name) {
        return columns.contains(name);
    }

    /**
     * Infer the predominant {@link ValueType} for each column by scanning all rows.
     * Blank cells are ignored; if all cells are blank, the column type is {@link ValueType#BLANK}.
     */
    public Map<String, ValueType> columnTypes() {
        Map<String, ValueType> types = new LinkedHashMap<>();
        for (String col : columns) {
            ValueType inferred = ValueType.BLANK;
            for (Map<String, CellValue> row : rows) {
                CellValue cv = row.get(col);
                if (cv != null && cv.type() != ValueType.BLANK) {
                    inferred = cv.type();
                    break; // use the first non-blank type
                }
            }
            types.put(col, inferred);
        }
        return types;
    }

    // ---------------------------------------------------------------
    // Row access
    // ---------------------------------------------------------------

    /** Get the row at the given index as an unmodifiable map. */
    public Map<String, CellValue> get(int index) {
        return rows.get(index);
    }

    /** Get the raw value at the given row and column. */
    public Object value(int row, String column) {
        CellValue cv = rows.get(row).get(column);
        return cv == null ? null : cv.value();
    }

    /** Get the CellValue at the given row and column. */
    public CellValue cellValue(int row, String column) {
        return rows.get(row).get(column);
    }

    /** Get all rows as an unmodifiable list. */
    public List<Map<String, CellValue>> toList() {
        return rows;
    }

    /** Get the first row, if present. */
    public Optional<Map<String, CellValue>> first() {
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    /** Get the last row, if present. */
    public Optional<Map<String, CellValue>> last() {
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(rows.size() - 1));
    }

    // ---------------------------------------------------------------
    // Column accessors (with coercion)
    // ---------------------------------------------------------------

    /** Extract a column as a list of CellValues. */
    public List<CellValue> column(String name) {
        validateColumn(name);
        return rows.stream()
                .map(row -> row.getOrDefault(name, CellValue.blank()))
                .collect(Collectors.toUnmodifiableList());
    }

    /** Extract a column as a list of coerced Strings. */
    public List<String> stringColumn(String name) {
        validateColumn(name);
        List<String> result = new ArrayList<>(rows.size());
        for (Map<String, CellValue> row : rows) {
            CellValue cv = row.get(name);
            result.add(cv == null || cv.isBlank() ? null : cv.asString());
        }
        return Collections.unmodifiableList(result);
    }

    /** Extract a column as a list of coerced Integers (null-safe). */
    public List<Integer> intColumn(String name) {
        validateColumn(name);
        List<Integer> result = new ArrayList<>(rows.size());
        for (Map<String, CellValue> row : rows) {
            CellValue cv = row.get(name);
            result.add(cv == null || cv.isBlank() ? null : cv.asInt());
        }
        return Collections.unmodifiableList(result);
    }

    /** Extract a column as a list of coerced Longs (null-safe). */
    public List<Long> longColumn(String name) {
        validateColumn(name);
        List<Long> result = new ArrayList<>(rows.size());
        for (Map<String, CellValue> row : rows) {
            CellValue cv = row.get(name);
            result.add(cv == null || cv.isBlank() ? null : cv.asLong());
        }
        return Collections.unmodifiableList(result);
    }

    /** Extract a column as a list of coerced Doubles (null-safe). */
    public List<Double> doubleColumn(String name) {
        validateColumn(name);
        List<Double> result = new ArrayList<>(rows.size());
        for (Map<String, CellValue> row : rows) {
            CellValue cv = row.get(name);
            result.add(cv == null || cv.isBlank() ? null : cv.asDouble());
        }
        return Collections.unmodifiableList(result);
    }

    /** Extract a column as a list of coerced Booleans (null-safe). */
    public List<Boolean> booleanColumn(String name) {
        validateColumn(name);
        List<Boolean> result = new ArrayList<>(rows.size());
        for (Map<String, CellValue> row : rows) {
            CellValue cv = row.get(name);
            result.add(cv == null || cv.isBlank() ? null : cv.asBoolean());
        }
        return Collections.unmodifiableList(result);
    }

    // ---------------------------------------------------------------
    // Transformations (return new Table)
    // ---------------------------------------------------------------

    /** Filter rows by a predicate on the row map. */
    public Table filter(Predicate<Map<String, CellValue>> predicate) {
        List<Map<String, CellValue>> filtered = rows.stream()
                .filter(predicate)
                .collect(Collectors.toList());
        return new Table(columns, filtered);
    }

    /** Select a subset of columns. */
    public Table select(String... columnNames) {
        return select(List.of(columnNames));
    }

    /** Select a subset of columns. */
    public Table select(List<String> columnNames) {
        for (String name : columnNames) {
            validateColumn(name);
        }
        List<Map<String, CellValue>> projected = rows.stream()
                .map(row -> {
                    Map<String, CellValue> newRow = new LinkedHashMap<>();
                    for (String col : columnNames) {
                        CellValue cv = row.get(col);
                        if (cv != null) newRow.put(col, cv);
                    }
                    return newRow;
                })
                .collect(Collectors.toList());
        return new Table(columnNames, projected);
    }

    /** Drop one or more columns. */
    public Table dropColumn(String... columnNames) {
        Set<String> toDrop = Set.of(columnNames);
        List<String> remaining = columns.stream()
                .filter(c -> !toDrop.contains(c))
                .collect(Collectors.toList());
        List<Map<String, CellValue>> projected = rows.stream()
                .map(row -> {
                    Map<String, CellValue> newRow = new LinkedHashMap<>(row);
                    toDrop.forEach(newRow::remove);
                    return newRow;
                })
                .collect(Collectors.toList());
        return new Table(remaining, projected);
    }

    /** Rename a column. */
    public Table renameColumn(String oldName, String newName) {
        validateColumn(oldName);
        List<String> newColumns = columns.stream()
                .map(c -> c.equals(oldName) ? newName : c)
                .collect(Collectors.toList());
        List<Map<String, CellValue>> newRows = rows.stream()
                .map(row -> {
                    Map<String, CellValue> newRow = new LinkedHashMap<>();
                    for (Map.Entry<String, CellValue> entry : row.entrySet()) {
                        String key = entry.getKey().equals(oldName) ? newName : entry.getKey();
                        newRow.put(key, entry.getValue());
                    }
                    return newRow;
                })
                .collect(Collectors.toList());
        return new Table(newColumns, newRows);
    }

    /** Add a computed column. */
    public Table addColumn(String name, Function<Map<String, CellValue>, CellValue> compute) {
        List<String> newColumns = new ArrayList<>(columns);
        newColumns.add(name);
        List<Map<String, CellValue>> newRows = rows.stream()
                .map(row -> {
                    Map<String, CellValue> newRow = new LinkedHashMap<>(row);
                    newRow.put(name, compute.apply(row));
                    return newRow;
                })
                .collect(Collectors.toList());
        return new Table(newColumns, newRows);
    }

    /** Sort rows by the natural ordering of a column's raw values. */
    @SuppressWarnings("unchecked")
    public Table sortBy(String columnName) {
        validateColumn(columnName);
        List<Map<String, CellValue>> sorted = new ArrayList<>(rows);
        sorted.sort((a, b) -> {
            CellValue va = a.get(columnName);
            CellValue vb = b.get(columnName);
            if (va == null || va.isBlank()) return vb == null || vb.isBlank() ? 0 : 1;
            if (vb == null || vb.isBlank()) return -1;
            Object oa = va.value();
            Object ob = vb.value();
            if (oa instanceof Comparable && ob instanceof Comparable) {
                return ((Comparable<Object>) oa).compareTo(ob);
            }
            return String.valueOf(oa).compareTo(String.valueOf(ob));
        });
        return new Table(columns, sorted);
    }

    /** Sort rows by a custom comparator on the row map. */
    public Table sortBy(Comparator<Map<String, CellValue>> comparator) {
        List<Map<String, CellValue>> sorted = new ArrayList<>(rows);
        sorted.sort(comparator);
        return new Table(columns, sorted);
    }

    /** Return the first n rows. */
    public Table head(int n) {
        return new Table(columns, rows.subList(0, Math.min(n, rows.size())));
    }

    /** Return the last n rows. */
    public Table tail(int n) {
        int start = Math.max(0, rows.size() - n);
        return new Table(columns, rows.subList(start, rows.size()));
    }

    /** Return a slice of rows [from, to). */
    public Table slice(int from, int to) {
        return new Table(columns, rows.subList(from, Math.min(to, rows.size())));
    }

    /** Remove duplicate rows (by value equality). */
    public Table distinct() {
        List<Map<String, CellValue>> unique = new ArrayList<>();
        Set<List<Object>> seen = new LinkedHashSet<>();
        for (Map<String, CellValue> row : rows) {
            List<Object> key = columns.stream()
                    .map(col -> {
                        CellValue cv = row.get(col);
                        return cv == null ? null : cv.value();
                    })
                    .collect(Collectors.toList());
            if (seen.add(key)) {
                unique.add(row);
            }
        }
        return new Table(columns, unique);
    }

    /** Concatenate another Table (must have the same columns). */
    public Table concat(Table other) {
        List<Map<String, CellValue>> combined = new ArrayList<>(this.rows.size() + other.rows.size());
        combined.addAll(this.rows);
        combined.addAll(other.rows);
        // Merge column lists preserving order, adding any new columns from other
        List<String> mergedColumns = new ArrayList<>(this.columns);
        for (String col : other.columns) {
            if (!mergedColumns.contains(col)) {
                mergedColumns.add(col);
            }
        }
        return new Table(mergedColumns, combined);
    }

    // ---------------------------------------------------------------
    // Conversion to typed Dataset
    // ---------------------------------------------------------------

    /**
     * Convert this Table to a typed {@link Dataset} of the given record class.
     *
     * <p>Column names are matched to record component names (case-sensitive).
     * Values are coerced to the component's declared type using {@link CellValue}
     * coercion rules.
     *
     * @param <T> the record type
     * @param recordClass the record class to convert to
     * @return a Dataset of the given type
     * @throws IllegalArgumentException if the class is not a record
     */
    public <T> Dataset<T> toDataset(Class<T> recordClass) {
        if (!recordClass.isRecord()) {
            throw new IllegalArgumentException("Class must be a record: " + recordClass.getName());
        }

        RecordComponent[] components = recordClass.getRecordComponents();
        Class<?>[] paramTypes = new Class<?>[components.length];
        for (int i = 0; i < components.length; i++) {
            paramTypes[i] = components[i].getType();
        }

        Constructor<T> ctor;
        try {
            ctor = recordClass.getDeclaredConstructor(paramTypes);
            ctor.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("No canonical constructor on " + recordClass.getName(), e);
        }

        List<T> records = new ArrayList<>(rows.size());
        for (Map<String, CellValue> row : rows) {
            Object[] args = new Object[components.length];
            for (int i = 0; i < components.length; i++) {
                RecordComponent comp = components[i];
                CellValue cv = row.get(comp.getName());
                args[i] = coerceToType(cv, comp.getType());
            }
            try {
                records.add(ctor.newInstance(args));
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Failed to create record instance: " + e.getMessage(), e);
            }
        }

        return Dataset.of(records);
    }

    /**
     * Convert this Table to a typed {@link Dataset}, using a static factory method
     * {@code Table.fromDataset()} in reverse. Column names are matched to record
     * components by name.
     */
    @SuppressWarnings("unchecked")
    public static <T> Table fromDataset(Dataset<T> dataset) {
        return dataset.toTable();
    }

    private static Object coerceToType(CellValue cv, Class<?> targetType) {
        if (cv == null || cv.isBlank()) {
            // Return default for primitives, null for objects
            if (targetType == int.class) return 0;
            if (targetType == long.class) return 0L;
            if (targetType == double.class) return 0.0;
            if (targetType == float.class) return 0.0f;
            if (targetType == boolean.class) return false;
            return null;
        }

        if (targetType == String.class) return cv.asString();
        if (targetType == int.class || targetType == Integer.class) return cv.asInt();
        if (targetType == long.class || targetType == Long.class) return cv.asLong();
        if (targetType == double.class || targetType == Double.class) return cv.asDouble();
        if (targetType == float.class || targetType == Float.class) return (float) cv.asDouble();
        if (targetType == boolean.class || targetType == Boolean.class) return cv.asBoolean();
        if (targetType == BigDecimal.class) return cv.asBigDecimal();
        if (targetType == LocalDate.class) return cv.asLocalDate();
        if (targetType == LocalDateTime.class) return cv.asLocalDateTime();
        if (targetType == java.time.LocalTime.class) return cv.asLocalTime();

        // Fallback: try returning the raw value if it's assignable
        Object raw = cv.value();
        if (raw != null && targetType.isInstance(raw)) return raw;

        // Last resort: string
        return cv.asString();
    }

    /** Map each row to a new row using the given function. */
    public Table mapRows(Function<Map<String, CellValue>, Map<String, CellValue>> mapper) {
        List<Map<String, CellValue>> mapped = rows.stream()
                .map(mapper)
                .collect(Collectors.toList());
        return new Table(columns, mapped);
    }

    /**
     * Unpivot (melt) value columns into rows.
     *
     * <p>Analogous to pandas {@code pd.melt()}: turns selected "value" columns into
     * two new columns ({@code variableColumn} and {@code valueColumn}), one row per
     * original value column, while repeating the "id" columns on every row.
     *
     * <pre>
     * // Input:
     * //   name  Q1  Q2  Q3
     * //   Alice 90  85  88
     *
     * table.melt(List.of("name"), List.of("Q1","Q2","Q3"), "quarter", "score")
     *
     * // Output:
     * //   name  quarter  score
     * //   Alice Q1       90
     * //   Alice Q2       85
     * //   Alice Q3       88
     * </pre>
     *
     * @param idColumns      columns to keep as identifiers (repeated on every output row)
     * @param valueColumns   columns to unpivot into rows
     * @param variableColumn name of the new column holding the original column names
     * @param valueColumn    name of the new column holding the values
     * @return a new melted Table
     */
    public Table melt(List<String> idColumns, List<String> valueColumns,
                      String variableColumn, String valueColumn) {
        for (String col : idColumns) validateColumn(col);
        for (String col : valueColumns) validateColumn(col);

        List<String> newColumns = new ArrayList<>(idColumns);
        newColumns.add(variableColumn);
        newColumns.add(valueColumn);

        List<Map<String, CellValue>> newRows = new ArrayList<>();
        for (Map<String, CellValue> row : rows) {
            for (String valCol : valueColumns) {
                Map<String, CellValue> newRow = new LinkedHashMap<>();
                // Copy id columns
                for (String idCol : idColumns) {
                    newRow.put(idCol, row.getOrDefault(idCol, CellValue.blank()));
                }
                // Variable = original column name
                newRow.put(variableColumn, CellValue.ofString(valCol));
                // Value = cell value from that column
                newRow.put(valueColumn, row.getOrDefault(valCol, CellValue.blank()));
                newRows.add(newRow);
            }
        }
        return new Table(newColumns, newRows);
    }

    /**
     * Convenience overload using default column names "variable" and "value".
     *
     * @see #melt(List, List, String, String)
     */
    public Table melt(List<String> idColumns, List<String> valueColumns) {
        return melt(idColumns, valueColumns, "variable", "value");
    }

    /**
     * Explode a column, expanding each row into multiple rows.
     *
     * <p>Analogous to pandas {@code DataFrame.explode()}: for each row, the
     * {@code exploder} function produces zero or more {@link CellValue}s from
     * the cell in the target column. Each produced value becomes a new row
     * with all other columns duplicated.
     *
     * <p>A common use case is splitting a delimited string:
     * <pre>
     * // Input:
     * //   name   tags
     * //   Alice  "java,python,sql"
     *
     * table.explode("tags", cv -&gt; {
     *     String s = cv.asString();
     *     return Arrays.stream(s.split(","))
     *             .map(CellValue::ofString)
     *             .collect(Collectors.toList());
     * })
     *
     * // Output:
     * //   name   tags
     * //   Alice  java
     * //   Alice  python
     * //   Alice  sql
     * </pre>
     *
     * @param columnName the column to explode
     * @param exploder   function that maps a CellValue to a list of CellValues
     * @return a new Table with exploded rows
     */
    public Table explode(String columnName, Function<CellValue, List<CellValue>> exploder) {
        validateColumn(columnName);

        List<Map<String, CellValue>> newRows = new ArrayList<>();
        for (Map<String, CellValue> row : rows) {
            CellValue cv = row.getOrDefault(columnName, CellValue.blank());
            List<CellValue> expanded;

            if (cv.isBlank()) {
                // Blank cell produces one row with blank in the exploded column
                expanded = List.of(CellValue.blank());
            } else {
                expanded = exploder.apply(cv);
                if (expanded == null || expanded.isEmpty()) {
                    // No values produced — drop the row (pandas behavior)
                    continue;
                }
            }

            for (CellValue newVal : expanded) {
                Map<String, CellValue> newRow = new LinkedHashMap<>(row);
                newRow.put(columnName, newVal);
                newRows.add(newRow);
            }
        }
        return new Table(columns, newRows);
    }

    // ---------------------------------------------------------------
    // Aggregation helpers
    // ---------------------------------------------------------------

    /** Check if any row matches the predicate. */
    public boolean any(Predicate<Map<String, CellValue>> predicate) {
        return rows.stream().anyMatch(predicate);
    }

    /** Check if all rows match the predicate. */
    public boolean all(Predicate<Map<String, CellValue>> predicate) {
        return rows.stream().allMatch(predicate);
    }

    /** Count rows matching the predicate. */
    public long count(Predicate<Map<String, CellValue>> predicate) {
        return rows.stream().filter(predicate).count();
    }

    // ---------------------------------------------------------------
    // Display
    // ---------------------------------------------------------------

    /**
     * Build a tabular string representation (similar to pandas DataFrame display).
     */
    public String toTabularString() {
        return toTabularString(20);
    }

    /**
     * Build a tabular string with the given maximum number of rows.
     */
    public String toTabularString(int maxRows) {
        if (columns.isEmpty()) return "Table[0 columns, 0 rows]";

        // Compute column widths
        int[] widths = new int[columns.size()];
        for (int c = 0; c < columns.size(); c++) {
            widths[c] = columns.get(c).length();
        }
        int displayRows = Math.min(maxRows, rows.size());
        for (int r = 0; r < displayRows; r++) {
            Map<String, CellValue> row = rows.get(r);
            for (int c = 0; c < columns.size(); c++) {
                CellValue cv = row.get(columns.get(c));
                String val = cv == null ? "" : cv.asString() == null ? "" : cv.asString();
                widths[c] = Math.max(widths[c], val.length());
            }
        }

        StringBuilder sb = new StringBuilder();

        // Header
        for (int c = 0; c < columns.size(); c++) {
            if (c > 0) sb.append("  ");
            sb.append(String.format("%-" + widths[c] + "s", columns.get(c)));
        }
        sb.append('\n');

        // Separator
        for (int c = 0; c < columns.size(); c++) {
            if (c > 0) sb.append("  ");
            sb.append("-".repeat(widths[c]));
        }
        sb.append('\n');

        // Rows
        for (int r = 0; r < displayRows; r++) {
            Map<String, CellValue> row = rows.get(r);
            for (int c = 0; c < columns.size(); c++) {
                if (c > 0) sb.append("  ");
                CellValue cv = row.get(columns.get(c));
                String val = cv == null ? "" : cv.asString() == null ? "" : cv.asString();
                sb.append(String.format("%-" + widths[c] + "s", val));
            }
            sb.append('\n');
        }

        if (rows.size() > maxRows) {
            sb.append("... ").append(rows.size() - maxRows).append(" more rows\n");
        }

        sb.append("[").append(rows.size()).append(" rows x ").append(columns.size()).append(" columns]");
        return sb.toString();
    }

    /** Print the table to stdout. */
    public void print() {
        System.out.println(toTabularString());
    }

    /** Print the table to stdout with the given max rows. */
    public void print(int maxRows) {
        System.out.println(toTabularString(maxRows));
    }

    @Override
    public String toString() {
        return toTabularString();
    }

    // ---------------------------------------------------------------
    // Builder
    // ---------------------------------------------------------------

    /**
     * Fluent builder for constructing a {@link Table}.
     *
     * <pre>
     * Table t = Table.builder()
     *     .columns("name", "age")
     *     .row("Alice", 30)
     *     .row("Bob", 25)
     *     .build();
     * </pre>
     */
    public static class Builder {
        private List<String> columns;
        private final List<Map<String, CellValue>> rows = new ArrayList<>();

        /** Set the column names. Must be called before adding rows. */
        public Builder columns(String... columnNames) {
            this.columns = List.of(columnNames);
            return this;
        }

        /** Set the column names. Must be called before adding rows. */
        public Builder columns(List<String> columnNames) {
            this.columns = List.copyOf(columnNames);
            return this;
        }

        /**
         * Add a row of raw values. Values are auto-wrapped into {@link CellValue}
         * instances based on their Java type:
         * <ul>
         *   <li>{@code String} &rarr; {@link ValueType#STRING}</li>
         *   <li>{@code Number} &rarr; {@link ValueType#NUMBER}</li>
         *   <li>{@code Boolean} &rarr; {@link ValueType#BOOLEAN}</li>
         *   <li>{@code LocalDate} &rarr; {@link ValueType#DATE}</li>
         *   <li>{@code LocalDateTime} &rarr; {@link ValueType#DATETIME}</li>
         *   <li>{@code LocalTime} &rarr; {@link ValueType#TIME}</li>
         *   <li>{@code null} &rarr; {@link ValueType#BLANK}</li>
         *   <li>Other &rarr; {@link ValueType#STRING} via {@code toString()}</li>
         * </ul>
         */
        public Builder row(Object... values) {
            if (columns == null) {
                throw new IllegalStateException("columns() must be called before row()");
            }
            if (values.length != columns.size()) {
                throw new IllegalArgumentException(
                        "Expected " + columns.size() + " values but got " + values.length);
            }
            Map<String, CellValue> row = new LinkedHashMap<>();
            for (int i = 0; i < values.length; i++) {
                row.put(columns.get(i), wrapValue(values[i]));
            }
            rows.add(row);
            return this;
        }

        /**
         * Add a row of pre-built CellValues.
         */
        public Builder rowCells(CellValue... cellValues) {
            if (columns == null) {
                throw new IllegalStateException("columns() must be called before rowCells()");
            }
            if (cellValues.length != columns.size()) {
                throw new IllegalArgumentException(
                        "Expected " + columns.size() + " values but got " + cellValues.length);
            }
            Map<String, CellValue> row = new LinkedHashMap<>();
            for (int i = 0; i < cellValues.length; i++) {
                row.put(columns.get(i), cellValues[i]);
            }
            rows.add(row);
            return this;
        }

        /** Build the Table. */
        public Table build() {
            if (columns == null) {
                throw new IllegalStateException("columns() must be called before build()");
            }
            return new Table(columns, rows);
        }
    }

    // ---------------------------------------------------------------
    // Internal helpers
    // ---------------------------------------------------------------

    private void validateColumn(String name) {
        if (!columns.contains(name)) {
            throw new IllegalArgumentException("Unknown column: '" + name +
                    "'. Available columns: " + columns);
        }
    }

    /**
     * Auto-wrap a raw Java value into a CellValue based on its runtime type.
     */
    public static CellValue wrapValue(Object value) {
        if (value == null) return CellValue.blank();
        if (value instanceof CellValue cv) return cv;
        if (value instanceof String s) return CellValue.ofString(s);
        if (value instanceof Boolean b) return CellValue.ofBoolean(b);
        if (value instanceof java.time.LocalDate ld) return CellValue.ofDate(ld);
        if (value instanceof java.time.LocalDateTime ldt) return CellValue.ofDateTime(ldt);
        if (value instanceof java.time.LocalTime lt) return CellValue.ofTime(lt);
        if (value instanceof Number n) return CellValue.ofNumber(n);
        // Fallback: treat as string
        return CellValue.ofString(value.toString());
    }
}
