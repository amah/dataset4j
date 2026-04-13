package dataset4j.poi;

import dataset4j.CellValue;
import dataset4j.Table;
import dataset4j.ValueType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CsvTableTest {

    @TempDir
    Path tempDir;

    @Test
    void writeAndReadRoundTrip() throws IOException {
        Path file = tempDir.resolve("roundtrip.csv");

        Table original = Table.builder()
                .columns("name", "age", "city")
                .row("Alice", 30, "NYC")
                .row("Bob", 25, "LA")
                .build();

        CsvDatasetWriter.toFile(file.toString()).writeTable(original);

        Table table = CsvDatasetReader.fromFile(file.toString()).readTable();

        assertEquals(2, table.size());
        assertEquals(List.of("name", "age", "city"), table.columns());

        // CSV reads everything as strings
        assertEquals(ValueType.STRING, table.cellValue(0, "name").type());
        assertEquals(ValueType.STRING, table.cellValue(0, "age").type());

        assertEquals("Alice", table.value(0, "name"));
        assertEquals("30", table.value(0, "age")); // string, not int

        // But coercion works
        assertEquals(30, table.cellValue(0, "age").asInt());
    }

    @Test
    void readWithCustomSeparator() throws IOException {
        Path file = tempDir.resolve("tabs.csv");

        Table original = Table.builder()
                .columns("a", "b")
                .row("hello", "world")
                .build();

        CsvDatasetWriter.toFile(file.toString())
                .separator('\t')
                .writeTable(original);

        Table table = CsvDatasetReader.fromFile(file.toString())
                .separator('\t')
                .readTable();

        assertEquals(1, table.size());
        assertEquals("hello", table.value(0, "a"));
        assertEquals("world", table.value(0, "b"));
    }

    @Test
    void readWithNoHeaders() throws IOException {
        Path file = tempDir.resolve("noheaders.csv");

        Table original = Table.builder()
                .columns("x", "y")
                .row("a", "b")
                .row("c", "d")
                .build();

        CsvDatasetWriter.toFile(file.toString())
                .headers(false)
                .writeTable(original);

        Table table = CsvDatasetReader.fromFile(file.toString())
                .headers(false)
                .readTable();

        assertEquals(2, table.size());
        assertEquals(List.of("Column1", "Column2"), table.columns());
        assertEquals("a", table.value(0, "Column1"));
    }

    @Test
    void handlesBlanks() throws IOException {
        Path file = tempDir.resolve("blanks.csv");

        Table original = Table.builder()
                .columns("a", "b")
                .row("hello", null)
                .row(null, "world")
                .build();

        CsvDatasetWriter.toFile(file.toString()).writeTable(original);

        Table table = CsvDatasetReader.fromFile(file.toString()).readTable();

        assertEquals(2, table.size());
        assertEquals("hello", table.value(0, "a"));
        assertTrue(table.cellValue(0, "b").isBlank());
        assertTrue(table.cellValue(1, "a").isBlank());
        assertEquals("world", table.value(1, "b"));
    }

    @Test
    void readThenTransform() throws IOException {
        Path file = tempDir.resolve("transform.csv");

        Table original = Table.builder()
                .columns("name", "score")
                .row("Alice", 90)
                .row("Bob", 70)
                .row("Charlie", 85)
                .build();

        CsvDatasetWriter.toFile(file.toString()).writeTable(original);

        Table table = CsvDatasetReader.fromFile(file.toString()).readTable();

        // Filter using coercion (CSV values are strings, coerce to int for comparison)
        Table highScores = table.filter(row -> row.get("score").asInt() >= 80);
        assertEquals(2, highScores.size());
    }

    @Test
    void writeEmpty() throws IOException {
        Path file = tempDir.resolve("empty.csv");
        Table empty = Table.empty("a", "b");

        CsvDatasetWriter.toFile(file.toString()).writeTable(empty);

        assertTrue(file.toFile().exists());
    }
}
