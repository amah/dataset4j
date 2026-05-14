package dataset4j.poi;

import dataset4j.Dataset;
import dataset4j.DatasetReadException;
import dataset4j.annotations.DataColumn;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

class StripTimezoneTest {

    @TempDir
    Path tempDir;

    public record StrictDateTime(
        @DataColumn(name = "ID", order = 1) int id,
        @DataColumn(name = "Ts", order = 2) LocalDateTime ts
    ) {}

    public record LenientDateTime(
        @DataColumn(name = "ID", order = 1) int id,
        @DataColumn(name = "Ts", order = 2, stripTimezone = true) LocalDateTime ts
    ) {}

    @Test
    void shouldFailByDefaultOnZonedString() throws IOException {
        Path file = writeTimestampSheet("strict.xlsx", "2026-05-13T14:30:00+02:00");

        assertThrows(DatasetReadException.class, () ->
            ExcelDatasetReader.fromFile(file.toString()).readAs(StrictDateTime.class));
    }

    @Test
    void shouldStripTimezoneViaAnnotation() throws IOException {
        Path file = writeTimestampSheet("annotated.xlsx", "2026-05-13T14:30:00+02:00");

        Dataset<LenientDateTime> ds = ExcelDatasetReader.fromFile(file.toString())
            .readAs(LenientDateTime.class);

        assertEquals(1, ds.size());
        assertEquals(LocalDateTime.of(2026, 5, 13, 14, 30, 0), ds.get(0).ts());
    }

    @Test
    void shouldStripTimezoneViaReaderFlag() throws IOException {
        Path file = writeTimestampSheet("reader_flag.xlsx", "2026-05-13T14:30:00+02:00");

        Dataset<StrictDateTime> ds = ExcelDatasetReader.fromFile(file.toString())
            .stripTimezone(true)
            .readAs(StrictDateTime.class);

        assertEquals(1, ds.size());
        assertEquals(LocalDateTime.of(2026, 5, 13, 14, 30, 0), ds.get(0).ts());
    }

    @Test
    void shouldStripTimezoneFromZuluForm() throws IOException {
        Path file = writeTimestampSheet("zulu.xlsx", "2026-05-13T14:30:00Z");

        Dataset<StrictDateTime> ds = ExcelDatasetReader.fromFile(file.toString())
            .stripTimezone(true)
            .readAs(StrictDateTime.class);

        assertEquals(LocalDateTime.of(2026, 5, 13, 14, 30, 0), ds.get(0).ts());
    }

    @Test
    void shouldStripTimezoneFromZonedRegionForm() throws IOException {
        // ZonedDateTime form, with a region in brackets
        Path file = writeTimestampSheet("zoned.xlsx", "2026-05-13T14:30:00+02:00[Europe/Paris]");

        Dataset<LenientDateTime> ds = ExcelDatasetReader.fromFile(file.toString())
            .readAs(LenientDateTime.class);

        assertEquals(LocalDateTime.of(2026, 5, 13, 14, 30, 0), ds.get(0).ts());
    }

    private Path writeTimestampSheet(String filename, String timestamp) throws IOException {
        Path file = tempDir.resolve(filename);
        try (var wb = new XSSFWorkbook();
             var fos = new FileOutputStream(file.toFile())) {
            var sheet = wb.createSheet();
            var header = sheet.createRow(0);
            header.createCell(0).setCellValue("ID");
            header.createCell(1).setCellValue("Ts");
            var row = sheet.createRow(1);
            row.createCell(0).setCellValue(1d);
            row.createCell(1).setCellValue(timestamp);
            wb.write(fos);
        }
        return file;
    }
}
