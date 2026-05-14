package dataset4j.parquet;

import dataset4j.CellValue;
import dataset4j.Dataset;
import dataset4j.Table;
import dataset4j.annotations.DataColumn;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ParquetInternValuesTest {

    @TempDir
    Path tempDir;

    public record Row(
        @DataColumn(name = "ID", order = 1) Integer id,
        @DataColumn(name = "Status", order = 2) String status,
        @DataColumn(name = "Amount", order = 3) BigDecimal amount
    ) {}

    @Test
    void shouldShareStringInstancesWhenInternEnabled() throws IOException {
        Path file = writeRepeated("intern_strings.parquet");

        Dataset<Row> ds = ParquetDatasetReader.fromFile(file.toString())
            .internValues(true)
            .readAs(Row.class);

        assertEquals(4, ds.size());
        assertSame(ds.get(0).status(), ds.get(1).status());
        assertSame(ds.get(0).status(), ds.get(3).status());
        assertNotSame(ds.get(0).status(), ds.get(2).status());
    }

    @Test
    void shouldShareBigDecimalInstancesWhenInternEnabled() throws IOException {
        Path file = writeRepeated("intern_bd.parquet");

        Dataset<Row> ds = ParquetDatasetReader.fromFile(file.toString())
            .internValues(true)
            .readAs(Row.class);

        assertSame(ds.get(0).amount(), ds.get(1).amount());
        assertSame(ds.get(0).amount(), ds.get(3).amount());
    }

    @Test
    void shouldShareCellValueInstancesInTableMode() throws IOException {
        Path file = writeRepeated("intern_table.parquet");

        Table t = ParquetDatasetReader.fromFile(file.toString())
            .internValues(true)
            .readTable();

        CellValue s0 = t.get(0).get("Status");
        CellValue s1 = t.get(1).get("Status");
        CellValue s3 = t.get(3).get("Status");

        assertSame(s0, s1);
        assertSame(s0, s3);
    }

    @Test
    void shouldNotShareBigDecimalsByDefault() throws IOException {
        Path file = writeRepeated("no_intern.parquet");

        Dataset<Row> ds = ParquetDatasetReader.fromFile(file.toString())
            .readAs(Row.class);

        assertEquals(ds.get(0).amount(), ds.get(1).amount());
        assertNotSame(ds.get(0).amount(), ds.get(1).amount());
    }

    /** Write a parquet with 4 rows: statuses {A, A, B, A}, amounts {100, 100, 200, 100}. */
    private Path writeRepeated(String filename) throws IOException {
        Path file = tempDir.resolve(filename);
        Dataset<Row> data = Dataset.of(
            new Row(1, "Active",   new BigDecimal("100.00")),
            new Row(2, "Active",   new BigDecimal("100.00")),
            new Row(3, "Inactive", new BigDecimal("200.00")),
            new Row(4, "Active",   new BigDecimal("100.00"))
        );
        ParquetDatasetWriter.toFile(file.toString())
            .withCompression(ParquetCompressionCodec.UNCOMPRESSED)
            .write(data);
        return file;
    }
}
