# CellValue Coercion Rules

`CellValue` typed accessors perform **coercion** rather than simple casting. This is essential because data from different sources has different internal representations: Excel stores all numbers as `Double`, CSV stores everything as `String`, and Parquet uses specific physical types.

## Coercion Table

| Target | From Number | From String | From Boolean | From Date/Time |
|--------|-------------|-------------|--------------|----------------|
| `asInt()` | `intValue()` | parse (strips `.0`) | `true`=1, `false`=0 | error |
| `asLong()` | `longValue()` | parse (strips `.0`) | `true`=1, `false`=0 | error |
| `asDouble()` | `doubleValue()` | parse | `true`=1.0, `false`=0.0 | error |
| `asBigDecimal()` | `BigDecimal.valueOf()` | `new BigDecimal()` | error | error |
| `asBoolean()` | non-zero=true | `"true"/"1"/"yes"`=true, `"false"/"0"/"no"`=false | direct | error |
| `asString()` | `toString()` | direct | `toString()` | `toString()` |
| `asLocalDate()` | error | `LocalDate.parse()` | error | direct or `.toLocalDate()` |
| `asLocalDateTime()` | error | `LocalDateTime.parse()` | error | direct or `.atStartOfDay()` |
| `asLocalTime()` | error | `LocalTime.parse()` | error | direct or `.toLocalTime()` |

## Null / Blank Handling

- Calling any typed accessor on a blank or null CellValue throws `CellCoercionException`
- `asString()` returns `null` for blank values (does not throw)
- Column accessors (`table.intColumn("x")`) return `null` in the list for blank cells

## Error Messages

`CellCoercionException` provides:

```java
exception.getSourceValue()  // the original value (e.g. "not-a-number")
exception.getSourceType()   // the ValueType (e.g. STRING)
exception.getTargetType()   // the requested type (e.g. "int")
exception.getMessage()      // "Cannot coerce STRING value 'not-a-number' to int"
```

## Source-Specific Notes

### Excel

Excel stores all numbers as `double` internally. When reading into a Table:
- Whole numbers (e.g. `30.0`) are stored as `Long` for cleaner display
- Fractional numbers stay as `Double`
- Date-formatted cells are stored as `LocalDate` or `LocalDateTime` (based on time component)
- The Excel format string (e.g. `"$#,##0.00"`, `"yyyy-mm-dd"`) is preserved in `CellValue.format()`

### Parquet

Parquet has a rich type system. Mappings:
- `BOOLEAN` to `ValueType.BOOLEAN`
- `INT32` to `ValueType.NUMBER` (as `Integer`), or `ValueType.DATE` if logical type is DATE
- `INT64` to `ValueType.NUMBER` (as `Long`)
- `FLOAT` to `ValueType.NUMBER` (as `Float`)
- `DOUBLE` to `ValueType.NUMBER` (as `Double`)
- `BYTE_ARRAY` with UTF8 logical type to `ValueType.STRING`
- `DECIMAL` logical type to `ValueType.NUMBER` (as `BigDecimal`)

### CSV

All CSV values are `ValueType.STRING` with `null` format. Rely on coercion accessors:

```java
Table csv = CsvDatasetReader.fromFile("data.csv").readTable();
int age = csv.cellValue(0, "age").asInt();       // coerces "30" to 30
double price = csv.cellValue(0, "price").asDouble(); // coerces "19.99" to 19.99
```
