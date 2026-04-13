package dataset4j.parquet;

import dataset4j.CellValue;
import dataset4j.Table;
import dataset4j.ValueType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ParquetTableTest {

    @TempDir
    Path tempDir;

    @Test
    void writeAndReadTableRoundTrip() throws IOException {
        Path file = tempDir.resolve("roundtrip.parquet");

        Table original = Table.builder()
                .columns("name", "age", "salary", "active")
                .row("Alice", 30, 75000.5, true)
                .row("Bob", 25, 55000.0, false)
                .row("Charlie", 35, 85000.25, true)
                .build();

        ParquetDatasetWriter.toFile(file.toString())
                .withCompression(ParquetCompressionCodec.UNCOMPRESSED)
                .writeTable(original);

        Table readBack = ParquetDatasetReader.fromFile(file.toString()).readTable();

        assertEquals(3, readBack.size());
        assertEquals(original.columns(), readBack.columns());

        assertEquals("Alice", readBack.cellValue(0, "name").asString());
        assertEquals("Bob", readBack.cellValue(1, "name").asString());
        assertTrue(readBack.cellValue(0, "active").asBoolean());
        assertFalse(readBack.cellValue(1, "active").asBoolean());
    }

    @Test
    void preservesValueTypes() throws IOException {
        Path file = tempDir.resolve("types.parquet");

        Table original = Table.builder()
                .columns("str", "int_val", "dbl_val", "flag")
                .row("hello", 42, 3.14, true)
                .build();

        ParquetDatasetWriter.toFile(file.toString())
                .withCompression(ParquetCompressionCodec.UNCOMPRESSED)
                .writeTable(original);

        Table table = ParquetDatasetReader.fromFile(file.toString()).readTable();

        assertEquals(ValueType.STRING, table.cellValue(0, "str").type());
        assertEquals(ValueType.NUMBER, table.cellValue(0, "int_val").type());
        assertEquals(ValueType.NUMBER, table.cellValue(0, "dbl_val").type());
        assertEquals(ValueType.BOOLEAN, table.cellValue(0, "flag").type());
    }

    @Test
    void handlesNullValues() throws IOException {
        Path file = tempDir.resolve("nulls.parquet");

        Table original = Table.builder()
                .columns("a", "b")
                .row("hello", null)
                .row(null, 42)
                .build();

        ParquetDatasetWriter.toFile(file.toString())
                .withCompression(ParquetCompressionCodec.UNCOMPRESSED)
                .writeTable(original);

        Table table = ParquetDatasetReader.fromFile(file.toString()).readTable();

        assertEquals(2, table.size());
        assertEquals("hello", table.cellValue(0, "a").asString());
        assertTrue(table.cellValue(0, "b").isBlank());
        assertTrue(table.cellValue(1, "a").isBlank());
        assertEquals(42, table.cellValue(1, "b").asInt());
    }

    @Test
    void handlesDateColumns() throws IOException {
        Path file = tempDir.resolve("dates.parquet");

        Table original = Table.builder()
                .columns("name", "birth")
                .rowCells(
                        CellValue.ofString("Alice"),
                        CellValue.ofDate(LocalDate.of(1990, 5, 15))
                )
                .rowCells(
                        CellValue.ofString("Bob"),
                        CellValue.ofDate(LocalDate.of(1985, 12, 25))
                )
                .build();

        ParquetDatasetWriter.toFile(file.toString())
                .withCompression(ParquetCompressionCodec.UNCOMPRESSED)
                .writeTable(original);

        Table table = ParquetDatasetReader.fromFile(file.toString()).readTable();

        assertEquals(ValueType.DATE, table.cellValue(0, "birth").type());
        assertEquals(LocalDate.of(1990, 5, 15), table.cellValue(0, "birth").value());
        assertEquals(LocalDate.of(1985, 12, 25), table.cellValue(1, "birth").value());
    }

    @Test
    void transformsAfterRead() throws IOException {
        Path file = tempDir.resolve("transform.parquet");

        Table original = Table.builder()
                .columns("name", "score")
                .row("Alice", 90)
                .row("Bob", 70)
                .row("Charlie", 85)
                .build();

        ParquetDatasetWriter.toFile(file.toString())
                .withCompression(ParquetCompressionCodec.UNCOMPRESSED)
                .writeTable(original);

        Table table = ParquetDatasetReader.fromFile(file.toString()).readTable();

        Table filtered = table
                .filter(row -> row.get("score").asInt() >= 80)
                .sortBy("name");

        assertEquals(2, filtered.size());
        assertEquals("Alice", filtered.value(0, "name"));
        assertEquals("Charlie", filtered.value(1, "name"));
    }

    @Test
    void coercesColumns() throws IOException {
        Path file = tempDir.resolve("coerce.parquet");

        Table original = Table.builder()
                .columns("count", "name")
                .row(10, "Alpha")
                .row(20, "Beta")
                .build();

        ParquetDatasetWriter.toFile(file.toString())
                .withCompression(ParquetCompressionCodec.UNCOMPRESSED)
                .writeTable(original);

        Table table = ParquetDatasetReader.fromFile(file.toString()).readTable();

        List<Integer> counts = table.intColumn("count");
        assertEquals(List.of(10, 20), counts);

        List<String> names = table.stringColumn("name");
        assertEquals(List.of("Alpha", "Beta"), names);
    }
}
