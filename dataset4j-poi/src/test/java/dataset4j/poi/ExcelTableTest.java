package dataset4j.poi;

import dataset4j.CellValue;
import dataset4j.Table;
import dataset4j.ValueType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ExcelTableTest {

    @TempDir
    Path tempDir;

    @Test
    void readTableBasic() throws IOException {
        // Write a typed dataset first, then read back as Table
        Path file = tempDir.resolve("basic.xlsx");
        Table original = Table.builder()
                .columns("Name", "Age", "Active")
                .row("Alice", 30, true)
                .row("Bob", 25, false)
                .build();

        ExcelDatasetWriter.toFile(file.toString()).writeTable(original);

        Table table = ExcelDatasetReader.fromFile(file.toString()).readTable();

        assertEquals(2, table.size());
        assertEquals(List.of("Name", "Age", "Active"), table.columns());
        assertEquals("Alice", table.value(0, "Name"));
        assertEquals(30L, table.value(0, "Age"));  // numbers stored as long when whole
        assertEquals(true, table.value(0, "Active"));
    }

    @Test
    void readTablePreservesTypes() throws IOException {
        Path file = tempDir.resolve("types.xlsx");
        Table original = Table.builder()
                .columns("text", "number", "decimal", "flag")
                .row("hello", 42, 3.14, true)
                .build();

        ExcelDatasetWriter.toFile(file.toString()).writeTable(original);

        Table table = ExcelDatasetReader.fromFile(file.toString()).readTable();

        assertEquals(ValueType.STRING, table.cellValue(0, "text").type());
        assertEquals(ValueType.NUMBER, table.cellValue(0, "number").type());
        assertEquals(ValueType.NUMBER, table.cellValue(0, "decimal").type());
        assertEquals(ValueType.BOOLEAN, table.cellValue(0, "flag").type());
    }

    @Test
    void readTablePreservesFormatString() throws IOException {
        Path file = tempDir.resolve("format.xlsx");

        // Write with format-bearing CellValues
        Table original = Table.builder()
                .columns("price")
                .rowCells(CellValue.ofNumber(1234.56, "$#,##0.00"))
                .build();

        ExcelDatasetWriter.toFile(file.toString()).writeTable(original);

        Table table = ExcelDatasetReader.fromFile(file.toString()).readTable();

        CellValue price = table.cellValue(0, "price");
        assertEquals(ValueType.NUMBER, price.type());
        assertNotNull(price.format());
        assertEquals("$#,##0.00", price.format());
    }

    @Test
    void readTableWithDates() throws IOException {
        Path file = tempDir.resolve("dates.xlsx");
        Table original = Table.builder()
                .columns("date")
                .rowCells(CellValue.ofDate(LocalDate.of(2024, 6, 15), "yyyy-mm-dd"))
                .build();

        ExcelDatasetWriter.toFile(file.toString()).writeTable(original);

        Table table = ExcelDatasetReader.fromFile(file.toString()).readTable();

        CellValue dateCell = table.cellValue(0, "date");
        assertEquals(ValueType.DATE, dateCell.type());
        assertEquals(LocalDate.of(2024, 6, 15), dateCell.value());
    }

    @Test
    void writeAndReadTableRoundTrip() throws IOException {
        Path file = tempDir.resolve("roundtrip.xlsx");

        Table original = Table.builder()
                .columns("id", "name", "salary", "active")
                .row(1, "Alice", 75000.50, true)
                .row(2, "Bob", 55000.0, false)
                .row(3, "Charlie", 85000.25, true)
                .build();

        ExcelDatasetWriter.toFile(file.toString())
                .sheet("Data")
                .writeTable(original);

        Table readBack = ExcelDatasetReader.fromFile(file.toString())
                .sheet("Data")
                .readTable();

        assertEquals(original.size(), readBack.size());
        assertEquals(original.columns(), readBack.columns());

        // Values should be preserved (types may differ slightly due to Excel representation)
        assertEquals("Alice", readBack.value(0, "name"));
        assertEquals("Bob", readBack.value(1, "name"));
        assertEquals(75000.5, readBack.cellValue(0, "salary").asDouble(), 0.01);
        assertTrue(readBack.cellValue(0, "active").asBoolean());
        assertFalse(readBack.cellValue(1, "active").asBoolean());
    }

    @Test
    void writeTableEmpty() throws IOException {
        Path file = tempDir.resolve("empty.xlsx");
        Table empty = Table.empty("a", "b");

        ExcelDatasetWriter.toFile(file.toString()).writeTable(empty);

        assertTrue(file.toFile().exists());
        assertTrue(file.toFile().length() > 0);
    }

    @Test
    void readTableWithBlankCells() throws IOException {
        Path file = tempDir.resolve("blanks.xlsx");
        Table original = Table.builder()
                .columns("a", "b")
                .row("hello", null)
                .row(null, 42)
                .build();

        ExcelDatasetWriter.toFile(file.toString()).writeTable(original);

        Table table = ExcelDatasetReader.fromFile(file.toString()).readTable();

        assertEquals(2, table.size());
        assertEquals("hello", table.value(0, "a"));
        assertTrue(table.cellValue(0, "b").isBlank());
        assertTrue(table.cellValue(1, "a").isBlank());
        assertEquals(42L, table.value(1, "b"));
    }

    @Test
    void writeTableWithNoHeaders() throws IOException {
        Path file = tempDir.resolve("noheaders.xlsx");
        Table original = Table.builder()
                .columns("a", "b")
                .row(1, 2)
                .row(3, 4)
                .build();

        ExcelDatasetWriter.toFile(file.toString())
                .headers(false)
                .writeTable(original);

        // Read back without headers — columns will be auto-generated
        Table table = ExcelDatasetReader.fromFile(file.toString())
                .headers(false)
                .readTable();

        assertEquals(2, table.size());
        assertEquals(List.of("Column1", "Column2"), table.columns());
        assertEquals(1L, table.value(0, "Column1"));
    }

    @Test
    void readTableCoercion() throws IOException {
        Path file = tempDir.resolve("coercion.xlsx");
        Table original = Table.builder()
                .columns("count", "name")
                .row(42, "Alice")
                .row(7, "Bob")
                .build();

        ExcelDatasetWriter.toFile(file.toString()).writeTable(original);

        Table table = ExcelDatasetReader.fromFile(file.toString()).readTable();

        // Coerce NUMBER column to int
        List<Integer> counts = table.intColumn("count");
        assertEquals(List.of(42, 7), counts);

        // Coerce STRING column
        List<String> names = table.stringColumn("name");
        assertEquals(List.of("Alice", "Bob"), names);
    }

    @Test
    void readTableThenTransform() throws IOException {
        Path file = tempDir.resolve("transform.xlsx");
        Table original = Table.builder()
                .columns("name", "age", "dept")
                .row("Alice", 30, "Eng")
                .row("Bob", 25, "Sales")
                .row("Charlie", 35, "Eng")
                .build();

        ExcelDatasetWriter.toFile(file.toString()).writeTable(original);

        Table table = ExcelDatasetReader.fromFile(file.toString()).readTable();

        // Filter, select, sort
        Table result = table
                .filter(row -> row.get("dept").asString().equals("Eng"))
                .select("name", "age")
                .sortBy("age");

        assertEquals(2, result.size());
        assertEquals("Alice", result.value(0, "name"));
        assertEquals("Charlie", result.value(1, "name"));
    }
}
