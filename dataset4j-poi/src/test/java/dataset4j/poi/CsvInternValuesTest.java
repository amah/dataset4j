package dataset4j.poi;

import dataset4j.CellValue;
import dataset4j.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CsvInternValuesTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldShareCellValueInstancesWhenInternEnabled() throws IOException {
        Path file = writeRepeatedCsv("intern.csv");

        Table t = CsvDatasetReader.fromFile(file.toString())
            .internValues(true)
            .readTable();

        CellValue s0 = t.get(0).get("status");
        CellValue s1 = t.get(1).get("status");
        CellValue s3 = t.get(3).get("status");

        assertSame(s0, s1, "Equal cell values must share one instance when interning is on");
        assertSame(s0, s3);
        assertNotSame(s0, t.get(2).get("status"));
    }

    @Test
    void shouldShareBlankCellValueAcrossRows() throws IOException {
        // Row 0 and 2 have empty notes → both should be the same BLANK CellValue instance.
        Path file = tempDir.resolve("blanks.csv");
        Files.writeString(file, """
            id,notes
            1,
            2,hello
            3,
            """);

        Table t = CsvDatasetReader.fromFile(file.toString())
            .internValues(true)
            .readTable();

        assertSame(t.get(0).get("notes"), t.get(2).get("notes"));
    }

    @Test
    void shouldNotShareCellValueByDefault() throws IOException {
        Path file = writeRepeatedCsv("no_intern.csv");

        Table t = CsvDatasetReader.fromFile(file.toString()).readTable();

        assertEquals(t.get(0).get("status"), t.get(1).get("status"));
        assertNotSame(t.get(0).get("status"), t.get(1).get("status"));
    }

    private Path writeRepeatedCsv(String filename) throws IOException {
        Path file = tempDir.resolve(filename);
        Files.writeString(file, """
            id,status,amount
            1,Active,100
            2,Active,100
            3,Inactive,200
            4,Active,100
            """);
        return file;
    }
}
