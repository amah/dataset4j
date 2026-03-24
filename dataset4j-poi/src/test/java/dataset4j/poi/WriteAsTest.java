package dataset4j.poi;

import dataset4j.Dataset;
import dataset4j.annotations.DataColumn;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

class WriteAsTest {

    @TempDir
    Path tempDir;

    public record DateAsString(
        @DataColumn(name = "Name", order = 1)
        String name,

        @DataColumn(name = "Start Date", order = 2, dateFormat = "dd/MM/yyyy",
                    writeAs = DataColumn.WriteAs.STRING)
        LocalDate startDate,

        @DataColumn(name = "Created", order = 3, dateFormat = "yyyy-MM-dd HH:mm",
                    writeAs = DataColumn.WriteAs.STRING)
        LocalDateTime created
    ) {}

    @Test
    void dateField_writeAsString_writesFormattedText() throws IOException {
        Path file = tempDir.resolve("date_as_string.xlsx");
        Dataset<DateAsString> data = Dataset.of(
            new DateAsString("Alice",
                LocalDate.of(2024, 3, 15),
                LocalDateTime.of(2024, 3, 15, 9, 30, 0))
        );

        ExcelDatasetWriter.toFile(file.toString()).write(data);

        try (Workbook wb = new XSSFWorkbook(new FileInputStream(file.toFile()))) {
            Row row = wb.getSheetAt(0).getRow(1);
            // Should be string cells, not numeric date serials
            assertEquals(CellType.STRING, row.getCell(1).getCellType());
            assertEquals("15/03/2024", row.getCell(1).getStringCellValue());

            assertEquals(CellType.STRING, row.getCell(2).getCellType());
            assertEquals("2024-03-15 09:30", row.getCell(2).getStringCellValue());
        }
    }

    public record MixedWriteAs(
        @DataColumn(name = "Date Native", order = 1)
        LocalDate dateNative,

        @DataColumn(name = "Date String", order = 2, dateFormat = "MM/dd/yyyy",
                    writeAs = DataColumn.WriteAs.STRING)
        LocalDate dateString
    ) {}

    @Test
    void autoVsString_sameRecord_differentCellTypes() throws IOException {
        Path file = tempDir.resolve("mixed.xlsx");
        LocalDate date = LocalDate.of(2025, 12, 25);
        Dataset<MixedWriteAs> data = Dataset.of(new MixedWriteAs(date, date));

        ExcelDatasetWriter.toFile(file.toString()).write(data);

        try (Workbook wb = new XSSFWorkbook(new FileInputStream(file.toFile()))) {
            Row row = wb.getSheetAt(0).getRow(1);
            // Native: numeric (Excel date serial)
            assertEquals(CellType.NUMERIC, row.getCell(0).getCellType());
            // String: text cell
            assertEquals(CellType.STRING, row.getCell(1).getCellType());
            assertEquals("12/25/2025", row.getCell(1).getStringCellValue());
        }
    }

    public record NumberAsString(
        @DataColumn(name = "Price", order = 1, numberFormat = "#,##0.00",
                    writeAs = DataColumn.WriteAs.STRING)
        BigDecimal price,

        @DataColumn(name = "Count", order = 2, writeAs = DataColumn.WriteAs.STRING)
        Integer count
    ) {}

    @Test
    void numberField_writeAsString_usesNumberFormatOrToString() throws IOException {
        Path file = tempDir.resolve("number_as_string.xlsx");
        Dataset<NumberAsString> data = Dataset.of(
            new NumberAsString(new BigDecimal("1234567.89"), 42)
        );

        ExcelDatasetWriter.toFile(file.toString()).write(data);

        try (Workbook wb = new XSSFWorkbook(new FileInputStream(file.toFile()))) {
            Row row = wb.getSheetAt(0).getRow(1);
            assertEquals(CellType.STRING, row.getCell(0).getCellType());
            assertEquals("1,234,567.89", row.getCell(0).getStringCellValue());

            assertEquals(CellType.STRING, row.getCell(1).getCellType());
            assertEquals("42", row.getCell(1).getStringCellValue());
        }
    }

    public record NullWithWriteAs(
        @DataColumn(name = "Date", order = 1, defaultValue = "N/A",
                    writeAs = DataColumn.WriteAs.STRING)
        LocalDate date
    ) {}

    @Test
    void nullValue_writeAsString_usesDefaultValue() throws IOException {
        Path file = tempDir.resolve("null_write_as.xlsx");
        Dataset<NullWithWriteAs> data = Dataset.of(new NullWithWriteAs(null));

        ExcelDatasetWriter.toFile(file.toString()).write(data);

        try (Workbook wb = new XSSFWorkbook(new FileInputStream(file.toFile()))) {
            Row row = wb.getSheetAt(0).getRow(1);
            assertEquals("N/A", row.getCell(0).getStringCellValue());
        }
    }

    public record DefaultWriteAs(
        @DataColumn(name = "Date", order = 1)
        LocalDate date,

        @DataColumn(name = "Value", order = 2)
        Integer value
    ) {}

    @Test
    void autoMode_noRegression_nativeTypesPreserved() throws IOException {
        Path file = tempDir.resolve("auto_mode.xlsx");
        Dataset<DefaultWriteAs> data = Dataset.of(
            new DefaultWriteAs(LocalDate.of(2024, 1, 1), 100)
        );

        ExcelDatasetWriter.toFile(file.toString()).write(data);

        try (Workbook wb = new XSSFWorkbook(new FileInputStream(file.toFile()))) {
            Row row = wb.getSheetAt(0).getRow(1);
            assertEquals(CellType.NUMERIC, row.getCell(0).getCellType());
            assertEquals(CellType.NUMERIC, row.getCell(1).getCellType());
        }
    }
}
