package dataset4j;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class TableTest {

    static final Table EMPLOYEES = Table.builder()
            .columns("name", "age", "dept", "salary")
            .row("Alice",   30, "Eng",   75000.0)
            .row("Bob",     25, "Sales", 55000.0)
            .row("Charlie", 35, "Eng",   85000.0)
            .row("Diana",   28, "Sales", 60000.0)
            .row("Eve",     32, "Eng",   80000.0)
            .build();

    // -----------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------

    @Nested class Construction {
        @Test void builderBasic() {
            Table t = Table.builder()
                    .columns("a", "b")
                    .row(1, "x")
                    .row(2, "y")
                    .build();
            assertEquals(2, t.size());
            assertEquals(List.of("a", "b"), t.columns());
        }

        @Test void empty() {
            Table t = Table.empty("a", "b");
            assertTrue(t.isEmpty());
            assertEquals(2, t.columnCount());
        }

        @Test void ofFactory() {
            List<Map<String, CellValue>> rows = List.of(
                    Map.of("x", CellValue.ofNumber(1)),
                    Map.of("x", CellValue.ofNumber(2))
            );
            Table t = Table.of(List.of("x"), rows);
            assertEquals(2, t.size());
        }

        @Test void builderAutoWrapsTypes() {
            Table t = Table.builder()
                    .columns("str", "num", "bool", "date", "dt", "time", "nil")
                    .row("hello", 42, true, LocalDate.of(2024, 1, 1),
                            LocalDateTime.of(2024, 1, 1, 12, 0), LocalTime.of(14, 30), null)
                    .build();

            assertEquals(ValueType.STRING, t.cellValue(0, "str").type());
            assertEquals(ValueType.NUMBER, t.cellValue(0, "num").type());
            assertEquals(ValueType.BOOLEAN, t.cellValue(0, "bool").type());
            assertEquals(ValueType.DATE, t.cellValue(0, "date").type());
            assertEquals(ValueType.DATETIME, t.cellValue(0, "dt").type());
            assertEquals(ValueType.TIME, t.cellValue(0, "time").type());
            assertEquals(ValueType.BLANK, t.cellValue(0, "nil").type());
        }

        @Test void builderRejectsWrongColumnCount() {
            var b = Table.builder().columns("a", "b");
            assertThrows(IllegalArgumentException.class, () -> b.row(1));
            assertThrows(IllegalArgumentException.class, () -> b.row(1, 2, 3));
        }

        @Test void builderRequiresColumns() {
            var b = Table.builder();
            assertThrows(IllegalStateException.class, () -> b.row(1));
            assertThrows(IllegalStateException.class, b::build);
        }
    }

    // -----------------------------------------------------------------
    // Schema inspection
    // -----------------------------------------------------------------

    @Nested class Schema {
        @Test void columns() {
            assertEquals(List.of("name", "age", "dept", "salary"), EMPLOYEES.columns());
        }

        @Test void columnCount() {
            assertEquals(4, EMPLOYEES.columnCount());
        }

        @Test void hasColumn() {
            assertTrue(EMPLOYEES.hasColumn("name"));
            assertFalse(EMPLOYEES.hasColumn("nonexistent"));
        }

        @Test void columnTypes() {
            Map<String, ValueType> types = EMPLOYEES.columnTypes();
            assertEquals(ValueType.STRING, types.get("name"));
            assertEquals(ValueType.NUMBER, types.get("age"));
            assertEquals(ValueType.STRING, types.get("dept"));
            assertEquals(ValueType.NUMBER, types.get("salary"));
        }
    }

    // -----------------------------------------------------------------
    // Row access
    // -----------------------------------------------------------------

    @Nested class RowAccess {
        @Test void getRow() {
            Map<String, CellValue> row = EMPLOYEES.get(0);
            assertEquals("Alice", row.get("name").asString());
            assertEquals(30, row.get("age").asInt());
        }

        @Test void value() {
            assertEquals("Alice", EMPLOYEES.value(0, "name"));
            assertEquals(30, EMPLOYEES.value(0, "age"));
        }

        @Test void cellValue() {
            CellValue cv = EMPLOYEES.cellValue(0, "salary");
            assertEquals(ValueType.NUMBER, cv.type());
            assertEquals(75000.0, cv.asDouble());
        }

        @Test void firstAndLast() {
            assertTrue(EMPLOYEES.first().isPresent());
            assertEquals("Alice", EMPLOYEES.first().get().get("name").asString());
            assertEquals("Eve", EMPLOYEES.last().get().get("name").asString());

            assertTrue(Table.empty("a").first().isEmpty());
        }
    }

    // -----------------------------------------------------------------
    // Column accessors with coercion
    // -----------------------------------------------------------------

    @Nested class ColumnAccessors {
        @Test void stringColumn() {
            List<String> names = EMPLOYEES.stringColumn("name");
            assertEquals(List.of("Alice", "Bob", "Charlie", "Diana", "Eve"), names);
        }

        @Test void intColumn() {
            List<Integer> ages = EMPLOYEES.intColumn("age");
            assertEquals(List.of(30, 25, 35, 28, 32), ages);
        }

        @Test void doubleColumn() {
            List<Double> salaries = EMPLOYEES.doubleColumn("salary");
            assertEquals(5, salaries.size());
            assertEquals(75000.0, salaries.get(0));
        }

        @Test void columnCellValues() {
            List<CellValue> col = EMPLOYEES.column("name");
            assertEquals(5, col.size());
            assertEquals(ValueType.STRING, col.get(0).type());
        }

        @Test void unknownColumnThrows() {
            assertThrows(IllegalArgumentException.class, () -> EMPLOYEES.stringColumn("nope"));
        }

        @Test void nullsInColumn() {
            Table t = Table.builder()
                    .columns("x")
                    .row((Object) null)
                    .row(42)
                    .build();
            List<Integer> col = t.intColumn("x");
            assertNull(col.get(0));
            assertEquals(42, col.get(1));
        }
    }

    // -----------------------------------------------------------------
    // CellValue coercion
    // -----------------------------------------------------------------

    @Nested class Coercion {
        @Test void numberToInt() {
            assertEquals(42, CellValue.ofNumber(42.7).asInt());
        }

        @Test void stringToInt() {
            assertEquals(123, CellValue.ofString("123").asInt());
        }

        @Test void stringWithDecimalToInt() {
            assertEquals(3, CellValue.ofString("3.0").asInt());
        }

        @Test void booleanToInt() {
            assertEquals(1, CellValue.ofBoolean(true).asInt());
            assertEquals(0, CellValue.ofBoolean(false).asInt());
        }

        @Test void numberToDouble() {
            assertEquals(42.5, CellValue.ofNumber(42.5).asDouble());
        }

        @Test void stringToDouble() {
            assertEquals(3.14, CellValue.ofString("3.14").asDouble(), 0.001);
        }

        @Test void numberToLong() {
            assertEquals(100L, CellValue.ofNumber(100).asLong());
        }

        @Test void toBigDecimal() {
            assertEquals(new BigDecimal("42.5"),
                    CellValue.ofString("42.5").asBigDecimal());
        }

        @Test void toBooleanFromString() {
            assertTrue(CellValue.ofString("true").asBoolean());
            assertTrue(CellValue.ofString("1").asBoolean());
            assertTrue(CellValue.ofString("yes").asBoolean());
            assertFalse(CellValue.ofString("false").asBoolean());
            assertFalse(CellValue.ofString("0").asBoolean());
            assertFalse(CellValue.ofString("no").asBoolean());
        }

        @Test void toBooleanFromNumber() {
            assertTrue(CellValue.ofNumber(1).asBoolean());
            assertFalse(CellValue.ofNumber(0).asBoolean());
        }

        @Test void toLocalDate() {
            assertEquals(LocalDate.of(2024, 1, 15),
                    CellValue.ofString("2024-01-15").asLocalDate());
        }

        @Test void toLocalDateTime() {
            assertEquals(LocalDateTime.of(2024, 1, 15, 10, 30),
                    CellValue.ofString("2024-01-15T10:30").asLocalDateTime());
        }

        @Test void localDateToLocalDateTime() {
            CellValue cv = CellValue.ofDate(LocalDate.of(2024, 1, 15));
            assertEquals(LocalDateTime.of(2024, 1, 15, 0, 0), cv.asLocalDateTime());
        }

        @Test void toLocalTime() {
            assertEquals(LocalTime.of(14, 30),
                    CellValue.ofString("14:30").asLocalTime());
        }

        @Test void coercionFailureGivesGoodMessage() {
            CellCoercionException ex = assertThrows(CellCoercionException.class,
                    () -> CellValue.ofString("not-a-number").asInt());
            assertEquals(ValueType.STRING, ex.getSourceType());
            assertEquals("int", ex.getTargetType());
            assertEquals("not-a-number", ex.getSourceValue());
        }

        @Test void blankCoercionThrows() {
            assertThrows(CellCoercionException.class, () -> CellValue.blank().asInt());
        }

        @Test void asStringNeverThrows() {
            assertEquals("42", CellValue.ofNumber(42).asString());
            assertEquals("true", CellValue.ofBoolean(true).asString());
            assertNull(CellValue.blank().asString());
        }
    }

    // -----------------------------------------------------------------
    // Transformations
    // -----------------------------------------------------------------

    @Nested class Transformations {
        @Test void filter() {
            Table eng = EMPLOYEES.filter(row -> "Eng".equals(row.get("dept").asString()));
            assertEquals(3, eng.size());
        }

        @Test void select() {
            Table projected = EMPLOYEES.select("name", "dept");
            assertEquals(List.of("name", "dept"), projected.columns());
            assertEquals(5, projected.size());
            assertFalse(projected.get(0).containsKey("age"));
        }

        @Test void dropColumn() {
            Table t = EMPLOYEES.dropColumn("salary");
            assertEquals(List.of("name", "age", "dept"), t.columns());
            assertFalse(t.get(0).containsKey("salary"));
        }

        @Test void renameColumn() {
            Table t = EMPLOYEES.renameColumn("name", "fullName");
            assertTrue(t.hasColumn("fullName"));
            assertFalse(t.hasColumn("name"));
            assertEquals("Alice", t.get(0).get("fullName").asString());
        }

        @Test void addColumn() {
            Table t = EMPLOYEES.addColumn("senior",
                    row -> CellValue.ofBoolean(row.get("age").asInt() >= 30));
            assertTrue(t.hasColumn("senior"));
            assertTrue(t.cellValue(0, "senior").asBoolean());  // Alice, 30
            assertFalse(t.cellValue(1, "senior").asBoolean()); // Bob, 25
        }

        @Test void sortBy() {
            Table sorted = EMPLOYEES.sortBy("age");
            assertEquals("Bob", sorted.value(0, "name"));      // 25
            assertEquals("Charlie", sorted.value(4, "name"));   // 35
        }

        @Test void head() {
            assertEquals(2, EMPLOYEES.head(2).size());
        }

        @Test void tail() {
            Table t = EMPLOYEES.tail(2);
            assertEquals(2, t.size());
            assertEquals("Diana", t.value(0, "name"));
            assertEquals("Eve", t.value(1, "name"));
        }

        @Test void slice() {
            Table t = EMPLOYEES.slice(1, 3);
            assertEquals(2, t.size());
            assertEquals("Bob", t.value(0, "name"));
            assertEquals("Charlie", t.value(1, "name"));
        }

        @Test void distinct() {
            Table t = Table.builder()
                    .columns("x")
                    .row(1)
                    .row(2)
                    .row(1)
                    .build()
                    .distinct();
            assertEquals(2, t.size());
        }

        @Test void concat() {
            Table a = Table.builder().columns("x").row(1).build();
            Table b = Table.builder().columns("x").row(2).build();
            Table c = a.concat(b);
            assertEquals(2, c.size());
            assertEquals(1, c.value(0, "x"));
            assertEquals(2, c.value(1, "x"));
        }

        @Test void mapRows() {
            Table upper = EMPLOYEES.mapRows(row -> {
                Map<String, CellValue> newRow = new LinkedHashMap<>(row);
                newRow.put("name", CellValue.ofString(row.get("name").asString().toUpperCase()));
                return newRow;
            });
            assertEquals("ALICE", upper.value(0, "name"));
        }

        @Test void melt() {
            Table scores = Table.builder()
                    .columns("name", "Q1", "Q2", "Q3")
                    .row("Alice", 90, 85, 88)
                    .row("Bob",   70, 75, 80)
                    .build();

            Table melted = scores.melt(
                    List.of("name"),
                    List.of("Q1", "Q2", "Q3"),
                    "quarter", "score");

            // 2 rows × 3 value columns = 6 rows
            assertEquals(6, melted.size());
            assertEquals(List.of("name", "quarter", "score"), melted.columns());

            // First row: Alice, Q1, 90
            assertEquals("Alice", melted.value(0, "name"));
            assertEquals("Q1", melted.value(0, "quarter"));
            assertEquals(90, melted.cellValue(0, "score").asInt());

            // Second row: Alice, Q2, 85
            assertEquals("Alice", melted.value(1, "name"));
            assertEquals("Q2", melted.value(1, "quarter"));
            assertEquals(85, melted.cellValue(1, "score").asInt());

            // Fourth row: Bob, Q1, 70
            assertEquals("Bob", melted.value(3, "name"));
            assertEquals("Q1", melted.value(3, "quarter"));
            assertEquals(70, melted.cellValue(3, "score").asInt());
        }

        @Test void meltDefaultColumnNames() {
            Table t = Table.builder()
                    .columns("id", "a", "b")
                    .row(1, "x", "y")
                    .build();

            Table melted = t.melt(List.of("id"), List.of("a", "b"));
            assertEquals(List.of("id", "variable", "value"), melted.columns());
            assertEquals(2, melted.size());
            assertEquals("a", melted.value(0, "variable"));
            assertEquals("x", melted.value(0, "value"));
            assertEquals("b", melted.value(1, "variable"));
            assertEquals("y", melted.value(1, "value"));
        }

        @Test void meltWithBlanks() {
            Table t = Table.builder()
                    .columns("id", "a", "b")
                    .row(1, "x", null)
                    .build();

            Table melted = t.melt(List.of("id"), List.of("a", "b"));
            assertEquals(2, melted.size());
            assertEquals("x", melted.value(0, "value"));
            assertTrue(melted.cellValue(1, "value").isBlank());
        }

        @Test void explodeDelimitedString() {
            Table t = Table.builder()
                    .columns("name", "tags")
                    .row("Alice", "java,python,sql")
                    .row("Bob", "go,rust")
                    .build();

            Table exploded = t.explode("tags", cv -> {
                String s = cv.asString();
                List<CellValue> result = new java.util.ArrayList<>();
                for (String part : s.split(",")) {
                    result.add(CellValue.ofString(part.trim()));
                }
                return result;
            });

            assertEquals(5, exploded.size());
            assertEquals(List.of("name", "tags"), exploded.columns());

            // Alice's tags
            assertEquals("Alice", exploded.value(0, "name"));
            assertEquals("java", exploded.value(0, "tags"));
            assertEquals("Alice", exploded.value(1, "name"));
            assertEquals("python", exploded.value(1, "tags"));
            assertEquals("Alice", exploded.value(2, "name"));
            assertEquals("sql", exploded.value(2, "tags"));

            // Bob's tags
            assertEquals("Bob", exploded.value(3, "name"));
            assertEquals("go", exploded.value(3, "tags"));
            assertEquals("Bob", exploded.value(4, "name"));
            assertEquals("rust", exploded.value(4, "tags"));
        }

        @Test void explodeBlankCellPreservesRow() {
            Table t = Table.builder()
                    .columns("name", "tags")
                    .row("Alice", "a,b")
                    .row("Bob", null)
                    .build();

            Table exploded = t.explode("tags", cv ->
                    java.util.Arrays.stream(cv.asString().split(","))
                            .map(CellValue::ofString)
                            .collect(java.util.stream.Collectors.toList()));

            // Alice → 2 rows, Bob (blank) → 1 row preserved
            assertEquals(3, exploded.size());
            assertEquals("Alice", exploded.value(0, "name"));
            assertEquals("a", exploded.value(0, "tags"));
            assertEquals("Alice", exploded.value(1, "name"));
            assertEquals("b", exploded.value(1, "tags"));
            assertEquals("Bob", exploded.value(2, "name"));
            assertTrue(exploded.cellValue(2, "tags").isBlank());
        }

        @Test void explodeEmptyResultDropsRow() {
            Table t = Table.builder()
                    .columns("name", "items")
                    .row("Alice", "keep")
                    .row("Bob", "drop-me")
                    .build();

            Table exploded = t.explode("items", cv -> {
                if ("drop-me".equals(cv.asString())) {
                    return List.of(); // empty → row dropped
                }
                return List.of(cv);
            });

            assertEquals(1, exploded.size());
            assertEquals("Alice", exploded.value(0, "name"));
        }

        @Test void meltThenExplode() {
            // Melt + explode compose naturally
            Table t = Table.builder()
                    .columns("name", "skills", "hobbies")
                    .row("Alice", "java,python", "chess,reading")
                    .build();

            Table melted = t.melt(
                    List.of("name"),
                    List.of("skills", "hobbies"),
                    "category", "items");

            // 1 row × 2 value columns = 2 rows
            assertEquals(2, melted.size());

            Table exploded = melted.explode("items", cv ->
                    java.util.Arrays.stream(cv.asString().split(","))
                            .map(s -> CellValue.ofString(s.trim()))
                            .collect(java.util.stream.Collectors.toList()));

            // skills: java, python (2) + hobbies: chess, reading (2) = 4
            assertEquals(4, exploded.size());
            assertEquals("skills", exploded.value(0, "category"));
            assertEquals("java", exploded.value(0, "items"));
            assertEquals("hobbies", exploded.value(2, "category"));
            assertEquals("chess", exploded.value(2, "items"));
        }
    }

    // -----------------------------------------------------------------
    // Aggregation helpers
    // -----------------------------------------------------------------

    @Nested class Aggregation {
        @Test void any() {
            assertTrue(EMPLOYEES.any(row -> row.get("age").asInt() > 34));
            assertFalse(EMPLOYEES.any(row -> row.get("age").asInt() > 100));
        }

        @Test void all() {
            assertTrue(EMPLOYEES.all(row -> row.get("age").asInt() > 20));
            assertFalse(EMPLOYEES.all(row -> row.get("age").asInt() > 30));
        }

        @Test void count() {
            assertEquals(3, EMPLOYEES.count(row -> "Eng".equals(row.get("dept").asString())));
        }
    }

    // -----------------------------------------------------------------
    // Display
    // -----------------------------------------------------------------

    @Nested class Display {
        @Test void toTabularString() {
            String s = EMPLOYEES.toTabularString();
            assertTrue(s.contains("name"));
            assertTrue(s.contains("Alice"));
            assertTrue(s.contains("5 rows x 4 columns"));
        }

        @Test void emptyTableDisplay() {
            Table t = Table.empty();
            assertEquals("Table[0 columns, 0 rows]", t.toTabularString());
        }

        @Test void truncation() {
            String s = EMPLOYEES.toTabularString(2);
            assertTrue(s.contains("3 more rows"));
        }
    }

    // -----------------------------------------------------------------
    // CellValue edge cases
    // -----------------------------------------------------------------

    @Nested class CellValueEdgeCases {
        @Test void blankQueries() {
            assertTrue(CellValue.blank().isBlank());
            assertFalse(CellValue.ofString("hello").isBlank());
        }

        @Test void hasFormat() {
            assertTrue(CellValue.ofNumber(42, "$#,##0.00").hasFormat());
            assertFalse(CellValue.ofNumber(42).hasFormat());
        }

        @Test void errorCellValue() {
            CellValue cv = CellValue.error("#DIV/0!");
            assertEquals(ValueType.ERROR, cv.type());
            assertEquals("#DIV/0!", cv.asString());
        }

        @Test void formulaCellValue() {
            CellValue cv = CellValue.ofFormula("=SUM(A1:A10)");
            assertEquals(ValueType.FORMULA, cv.type());
            assertEquals("=SUM(A1:A10)", cv.value());
        }

        @Test void toStringFormat() {
            assertEquals("CellValue[BLANK]", CellValue.blank().toString());
            assertTrue(CellValue.ofNumber(42, "$#,##0").toString().contains("$#,##0"));
        }

        @Test void equality() {
            assertEquals(CellValue.ofNumber(42), CellValue.ofNumber(42));
            assertNotEquals(CellValue.ofNumber(42), CellValue.ofString("42"));
            assertEquals(CellValue.blank(), CellValue.blank());
        }
    }
}
