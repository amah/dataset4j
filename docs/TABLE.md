# Table — Untyped Tabular Data

`Table` is a schema-free, immutable container for tabular data. Where `Dataset<T>` requires a Java record class, `Table` stores rows as maps of column name to `CellValue`, preserving source type information and format metadata from the originating file (Excel, Parquet, CSV).

## When to use Table vs Dataset\<T\>

| Use case | Use |
|----------|-----|
| You know the schema upfront and have a record class | `Dataset<T>` |
| You want to load a file without defining a record | `Table` |
| You need to inspect, rename, or reshape columns dynamically | `Table` |
| You want to preserve Excel formatting for round-trips | `Table` |
| You want type-safe, compile-time-checked field access | `Dataset<T>` |
| You need an intermediate format between files (Excel to Parquet) | `Table` |

Both can be converted to each other freely via `dataset.toTable()` and `table.toDataset(MyRecord.class)`.

## Core Types

### ValueType

A normalized type classifier covering all sources:

```java
public enum ValueType {
    STRING, NUMBER, BOOLEAN,
    DATE, DATETIME, TIME,
    FORMULA, BLANK, ERROR
}
```

| Source | Mapping |
|--------|---------|
| Excel | POI CellType to ValueType, format string preserved (e.g. `$#,##0.00`, `yyyy-mm-dd`) |
| Parquet | Physical/logical type to ValueType (INT32/DATE to DATE, BOOLEAN to BOOLEAN, etc.) |
| CSV | All values are STRING with null format |

### CellValue

Each cell in a Table is a `CellValue` record carrying three pieces of information:

```java
public record CellValue(Object value, ValueType type, String format) { }
```

- **value** — the raw Java value (null for BLANK)
- **type** — the normalized source type
- **format** — the source format string (e.g. Excel `"$#,##0.00"`) or null

#### Factories

```java
CellValue.ofString("hello")
CellValue.ofNumber(42)
CellValue.ofNumber(1234.56, "$#,##0.00")   // with format
CellValue.ofBoolean(true)
CellValue.ofDate(LocalDate.of(2024, 1, 15))
CellValue.ofDate(LocalDate.of(2024, 1, 15), "yyyy-mm-dd")
CellValue.ofDateTime(LocalDateTime.now())
CellValue.ofTime(LocalTime.of(14, 30))
CellValue.ofFormula("=SUM(A1:A10)")
CellValue.error("#DIV/0!")
CellValue.blank()
CellValue.of(value, ValueType.NUMBER, "$#,##0")  // generic factory
```

#### Typed Accessors (Coercion)

Accessors **coerce** rather than cast. For example, an Excel numeric `Double 30.0` coerces to `int 30`, and a CSV string `"123"` coerces to `int 123`.

```java
cv.asString()        // always works (null for blank)
cv.asInt()           // Number.intValue(), parses strings, Boolean 1/0
cv.asLong()
cv.asDouble()
cv.asBigDecimal()
cv.asBoolean()       // "true"/"1"/"yes" and "false"/"0"/"no"
cv.asLocalDate()
cv.asLocalDateTime()
cv.asLocalTime()
```

Coercion failures throw `CellCoercionException` with the source value, source type, and target type:

```
Cannot coerce STRING value 'not-a-number' to int
```

#### Query Helpers

```java
cv.isBlank()     // true for null value or BLANK type
cv.hasFormat()   // true if format string is non-null and non-empty
```

## Creating a Table

### Builder (most common)

```java
Table table = Table.builder()
    .columns("name", "age", "dept")
    .row("Alice", 30, "Eng")        // auto-wrapped: String->STRING, int->NUMBER
    .row("Bob", 25, "Sales")
    .build();
```

Raw Java values are auto-wrapped based on type: `String` to STRING, `Number` to NUMBER, `Boolean` to BOOLEAN, `LocalDate` to DATE, `LocalDateTime` to DATETIME, `LocalTime` to TIME, `null` to BLANK.

For explicit control, use `rowCells()`:

```java
Table table = Table.builder()
    .columns("price", "date")
    .rowCells(
        CellValue.ofNumber(1234.56, "$#,##0.00"),
        CellValue.ofDate(LocalDate.of(2024, 6, 15), "yyyy-mm-dd")
    )
    .build();
```

### Factory methods

```java
Table.of(List.of("a", "b"), listOfRowMaps)
Table.empty("a", "b")
Table.empty(List.of("a", "b"))
```

### From a Dataset

```java
Dataset<Employee> ds = Dataset.of(...);
Table table = ds.toTable();
// or equivalently:
Table table = Table.fromDataset(ds);
```

## Reading from Files

### Excel

```java
Table table = ExcelDatasetReader.fromFile("data.xlsx")
    .sheet("Sheet1")       // optional
    .readTable();

// CellValues preserve Excel cell types and format strings
CellValue price = table.cellValue(0, "price");
price.type();    // ValueType.NUMBER
price.format();  // "$#,##0.00"
price.asDouble() // 1234.56
```

### Parquet

```java
Table table = ParquetDatasetReader.fromFile("data.parquet")
    .readTable();

// Parquet schema types are mapped to ValueType
table.cellValue(0, "birth_date").type();  // ValueType.DATE
```

### CSV

```java
Table table = CsvDatasetReader.fromFile("data.csv")
    .separator(',')        // default
    .readTable();

// All CSV values are STRING — use coercion to get typed values
table.cellValue(0, "age").type();       // ValueType.STRING
table.cellValue(0, "age").asInt();      // coerces "30" to 30
```

### Via DatasetIO facade

```java
Table t1 = DatasetIO.excel().fromFile("data.xlsx").readTable();
Table t2 = DatasetIO.parquet().fromFile("data.parquet").readTable();
Table t3 = DatasetIO.csv().fromFile("data.csv").readTable();
```

## Writing to Files

### Excel (preserves types and formats)

```java
ExcelDatasetWriter.toFile("output.xlsx")
    .sheet("Report")
    .writeTable(table);
```

CellValues with format strings are written with the corresponding Excel number format, so `CellValue.ofNumber(1234.56, "$#,##0.00")` renders as `$1,234.56` in Excel.

### Parquet (types inferred from first non-blank row)

```java
ParquetDatasetWriter.toFile("output.parquet")
    .withCompression(ParquetCompressionCodec.SNAPPY)
    .writeTable(table);
```

### CSV (all values stringified)

```java
CsvDatasetWriter.toFile("output.csv")
    .separator(',')
    .writeTable(table);
```

## Schema Inspection

```java
table.columns()       // List<String> — ordered column names
table.columnCount()   // int
table.size()          // row count
table.isEmpty()
table.hasColumn("x")
table.columnTypes()   // Map<String, ValueType> — inferred from first non-blank value
```

## Data Access

### Row access

```java
table.get(0)                    // Map<String, CellValue> — full row
table.first()                   // Optional<Map<String, CellValue>>
table.last()
table.value(0, "name")         // Object — raw value
table.cellValue(0, "name")     // CellValue — with type + format
```

### Column accessors (with coercion)

```java
table.column("name")           // List<CellValue>
table.stringColumn("name")    // List<String>   — coerced, null for blanks
table.intColumn("age")        // List<Integer>  — coerced, null for blanks
table.longColumn("id")        // List<Long>
table.doubleColumn("salary")  // List<Double>
table.booleanColumn("active") // List<Boolean>
```

## Transformations

All transformations return new Table instances (immutable).

### Filter

```java
Table engineers = table.filter(row -> "Eng".equals(row.get("dept").asString()));
```

### Select / Drop columns

```java
Table projected = table.select("name", "dept");     // keep only these
Table reduced = table.dropColumn("internal_id");     // remove these
```

### Rename

```java
Table renamed = table.renameColumn("name", "fullName");
```

### Add computed column

```java
Table withSenior = table.addColumn("senior",
    row -> CellValue.ofBoolean(row.get("age").asInt() >= 30));
```

### Sort

```java
Table sorted = table.sortBy("age");                          // natural order
Table sorted = table.sortBy((a, b) -> /* custom comparator */);
```

### Slice

```java
table.head(10)
table.tail(5)
table.slice(2, 5)    // rows [2, 5)
```

### Deduplication

```java
table.distinct()    // by value equality across all columns
```

### Concatenation

```java
Table combined = table1.concat(table2);   // columns are merged
```

### Map rows

```java
Table upper = table.mapRows(row -> {
    Map<String, CellValue> newRow = new LinkedHashMap<>(row);
    newRow.put("name", CellValue.ofString(row.get("name").asString().toUpperCase()));
    return newRow;
});
```

### Melt (unpivot)

Turns selected columns into rows — the inverse of pivot.

```java
// Input:
//   name   Q1  Q2  Q3
//   Alice  90  85  88
//   Bob    70  75  80

Table melted = table.melt(
    List.of("name"),                    // id columns (kept as-is)
    List.of("Q1", "Q2", "Q3"),         // value columns (unpivoted)
    "quarter",                          // new column for original column names
    "score"                             // new column for the values
);

// Output:
//   name   quarter  score
//   Alice  Q1       90
//   Alice  Q2       85
//   Alice  Q3       88
//   Bob    Q1       70
//   Bob    Q2       75
//   Bob    Q3       80
```

A convenience overload uses default column names "variable" and "value":

```java
table.melt(List.of("id"), List.of("a", "b"))
// columns: id, variable, value
```

### Explode

Expands a cell into multiple rows, one per element, duplicating all other columns.

```java
// Input:
//   name   tags
//   Alice  "java,python,sql"
//   Bob    "go,rust"

Table exploded = table.explode("tags", cv -> {
    return Arrays.stream(cv.asString().split(","))
            .map(s -> CellValue.ofString(s.trim()))
            .collect(Collectors.toList());
});

// Output:
//   name   tags
//   Alice  java
//   Alice  python
//   Alice  sql
//   Bob    go
//   Bob    rust
```

Behavior for edge cases:
- **Blank cell** — preserves the row with a blank value in the exploded column
- **Empty list returned** — drops the row (matches pandas behavior)

Melt and explode compose naturally:

```java
table.melt(List.of("name"), List.of("skills", "hobbies"), "category", "items")
     .explode("items", cv -> Arrays.stream(cv.asString().split(","))
             .map(CellValue::ofString).collect(Collectors.toList()));
```

## Aggregation Helpers

```java
table.any(row -> row.get("age").asInt() > 30)    // boolean
table.all(row -> row.get("age").asInt() > 20)    // boolean
table.count(row -> "Eng".equals(row.get("dept").asString()))  // long
```

## Conversion to Dataset\<T\>

```java
record Employee(String name, int age, String dept) {}

Dataset<Employee> ds = table.toDataset(Employee.class);
```

Column names are matched to record component names (case-sensitive). Values are coerced using CellValue coercion rules:

- `CellValue(30.0, NUMBER)` to `int` field: coerces via `intValue()` to `30`
- `CellValue("123", STRING)` to `int` field: parses to `123`
- `CellValue("true", STRING)` to `boolean` field: coerces to `true`
- Blank/null to primitive field: returns Java default (`0`, `false`, etc.)
- Blank/null to object field: returns `null`

### Round-trip

```java
// Dataset -> Table -> transform -> Dataset
Dataset<Employee> result = employees.toTable()
    .filter(row -> "Eng".equals(row.get("dept").asString()))
    .sortBy("age")
    .toDataset(Employee.class);
```

### Cross-format conversion

```java
// Read Excel as Table, write as Parquet
Table table = ExcelDatasetReader.fromFile("input.xlsx").readTable();
ParquetDatasetWriter.toFile("output.parquet").writeTable(table);

// Read CSV as Table, convert to typed Dataset
Table csv = CsvDatasetReader.fromFile("data.csv").readTable();
Dataset<Employee> employees = csv.toDataset(Employee.class);
```

## Display

```java
table.print()         // print to stdout (max 20 rows)
table.print(50)       // print to stdout (max 50 rows)
table.toTabularString()
table.toString()      // same as toTabularString()
```

Output:

```
name     age  dept   salary
-------  ---  -----  ------
Alice    30   Eng    75000.0
Bob      25   Sales  55000.0
Charlie  35   Eng    85000.0
... 2 more rows
[5 rows x 4 columns]
```
