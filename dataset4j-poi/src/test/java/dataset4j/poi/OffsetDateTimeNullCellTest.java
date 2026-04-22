package dataset4j.poi;

import dataset4j.Dataset;
import dataset4j.annotations.DataColumn;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Reader behaviour for OffsetDateTime columns with missing / blank / empty cells.
 *
 * <p>Without a default, the cell maps to {@code null}. With
 * {@code @DataColumn(defaultValue=...)}, the annotation string must be parsed into a
 * real {@code OffsetDateTime} — otherwise the raw {@code String} reaches the record
 * canonical constructor and reflection rejects it with
 * {@code IllegalArgumentException: argument type mismatch} (a String → OffsetDateTime
 * type mismatch at construction time).
 */
class OffsetDateTimeNullCellTest {

    public record Event(
        @DataColumn(name = "ID", order = 1)
        Integer id,

        @DataColumn(name = "Timestamp", order = 2)
        OffsetDateTime timestamp
    ) {}

    @TempDir
    Path tempDir;

    /**
     * Build an xlsx where:
     * row 1: [1, 2024-01-15T10:30:00Z]  (populated)
     * row 2: [2, <missing cell — never created>]
     * row 3: [3, <blank cell — created then left blank>]
     * row 4: [4, <empty string cell>]
     */
    private Path writeFixture() throws IOException {
        Path file = tempDir.resolve("offsetdatetime_nulls.xlsx");
        try (Workbook wb = new XSSFWorkbook()) {
            Sheet sheet = wb.createSheet("Events");

            Row header = sheet.createRow(0);
            header.createCell(0).setCellValue("ID");
            header.createCell(1).setCellValue("Timestamp");

            Row r1 = sheet.createRow(1);
            r1.createCell(0).setCellValue(1);
            r1.createCell(1).setCellValue("2024-01-15T10:30:00Z");

            Row r2 = sheet.createRow(2);
            r2.createCell(0).setCellValue(2);
            // no cell at index 1 — row.getCell(1) returns null

            Row r3 = sheet.createRow(3);
            r3.createCell(0).setCellValue(3);
            Cell blank = r3.createCell(1);
            blank.setBlank();

            Row r4 = sheet.createRow(4);
            r4.createCell(0).setCellValue(4);
            r4.createCell(1).setCellValue("");

            try (FileOutputStream out = new FileOutputStream(file.toFile())) {
                wb.write(out);
            }
        }
        return file;
    }

    @Test
    void shouldReturnNullForMissingBlankOrEmptyOffsetDateTimeCells() throws IOException {
        Path file = writeFixture();

        Dataset<Event> read = ExcelDatasetReader
            .fromFile(file.toString())
            .readAs(Event.class);

        assertEquals(4, read.size());

        Event populated = read.toList().get(0);
        assertEquals(1, populated.id());
        assertEquals(OffsetDateTime.of(2024, 1, 15, 10, 30, 0, 0, ZoneOffset.UTC), populated.timestamp());

        Event missing = read.toList().get(1);
        assertEquals(2, missing.id());
        assertNull(missing.timestamp(), "missing cell should map to null");

        Event blank = read.toList().get(2);
        assertEquals(3, blank.id());
        assertNull(blank.timestamp(), "blank cell should map to null");

        Event empty = read.toList().get(3);
        assertEquals(4, empty.id());
        assertNull(empty.timestamp(), "empty string cell should map to null");
    }

    public record EventWithDefault(
        @DataColumn(name = "ID", order = 1)
        Integer id,

        @DataColumn(name = "Timestamp", order = 2, defaultValue = "2000-01-01T00:00:00Z")
        OffsetDateTime timestamp
    ) {}

    @Test
    void shouldApplyAnnotationDefaultValueForMissingOffsetDateTimeCell() throws IOException {
        Path file = writeFixture();

        Dataset<EventWithDefault> read = ExcelDatasetReader
            .fromFile(file.toString())
            .readAs(EventWithDefault.class);

        OffsetDateTime expected = OffsetDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

        assertEquals(expected, read.toList().get(1).timestamp(), "missing cell should use annotation default");
        assertEquals(expected, read.toList().get(2).timestamp(), "blank cell should use annotation default");
        assertEquals(expected, read.toList().get(3).timestamp(), "empty string cell should use annotation default");
    }

    public record EventForRoundTrip(
        @DataColumn(name = "ID", order = 1)
        Integer id,

        @DataColumn(name = "Timestamp", order = 2)
        OffsetDateTime timestamp
    ) {}

    @Test
    void shouldRoundTripOffsetDateTimeThroughWriterAndReader() throws IOException {
        Path file = tempDir.resolve("offsetdatetime_roundtrip.xlsx");

        Dataset<EventForRoundTrip> original = Dataset.of(
            new EventForRoundTrip(1, OffsetDateTime.of(2024, 1, 15, 10, 30, 0, 0, ZoneOffset.UTC)),
            new EventForRoundTrip(2, null)
        );

        ExcelDatasetWriter.toFile(file.toString()).write(original);

        Dataset<EventForRoundTrip> read = ExcelDatasetReader
            .fromFile(file.toString())
            .readAs(EventForRoundTrip.class);

        assertEquals(2, read.size());
        assertEquals(OffsetDateTime.of(2024, 1, 15, 10, 30, 0, 0, ZoneOffset.UTC),
            read.toList().get(0).timestamp());
        assertNull(read.toList().get(1).timestamp());
    }
}
