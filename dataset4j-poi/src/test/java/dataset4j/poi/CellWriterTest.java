package dataset4j.poi;

import dataset4j.Dataset;
import dataset4j.annotations.DataColumn;
import dataset4j.annotations.FieldMeta;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class CellWriterTest {

    @TempDir
    Path tempDir;

    public record Item(
        @DataColumn(name = "Name", order = 1)
        String name,

        @DataColumn(name = "Price", order = 2, numberFormat = "$#,##0.00")
        BigDecimal price,

        @DataColumn(name = "Quantity", order = 3)
        Integer quantity
    ) {}

    @Test
    void defaultBehavior_noCustomWriter() throws IOException {
        Path file = tempDir.resolve("default.xlsx");
        Dataset<Item> data = Dataset.of(
            new Item("Widget", new BigDecimal("9.99"), 5)
        );

        ExcelDatasetWriter.toFile(file.toString()).write(data);

        Dataset<Item> read = ExcelDatasetReader.fromFile(file.toString())
            .hasHeaders(true).read(Item.class);

        assertEquals(1, read.size());
        Item item = read.first().orElseThrow();
        assertEquals("Widget", item.name());
        assertEquals(0, new BigDecimal("9.99").compareTo(item.price()));
        assertEquals(Integer.valueOf(5), item.quantity());
    }

    @Test
    void globalCellWriter_modifiesAllCells() throws IOException {
        Path file = tempDir.resolve("global.xlsx");
        Dataset<Item> data = Dataset.of(
            new Item("A", new BigDecimal("1"), 2)
        );

        ExcelDatasetWriter.toFile(file.toString())
            .cellWriter(ctx -> {
                // Write all string values uppercased, delegate rest
                if (ctx.getValue() instanceof String s) {
                    ctx.getCell().setCellValue(s.toUpperCase());
                    ctx.getCell().setCellStyle(ctx.getColumnStyle());
                } else {
                    ctx.writeDefault();
                }
            })
            .write(data);

        // Verify by reading raw cell value
        try (Workbook wb = new XSSFWorkbook(new FileInputStream(file.toFile()))) {
            Sheet sheet = wb.getSheetAt(0);
            Row row = sheet.getRow(1); // data row
            assertEquals("A", row.getCell(0).getStringCellValue()); // "A".toUpperCase() == "A"
            assertEquals(1.0, row.getCell(1).getNumericCellValue());
            assertEquals(2.0, row.getCell(2).getNumericCellValue());
        }
    }

    @Test
    void perFieldCellWriter_onlyAffectsTargetField() throws IOException {
        Path file = tempDir.resolve("perfield.xlsx");
        Dataset<Item> data = Dataset.of(
            new Item("Gadget", new BigDecimal("19.99"), 10)
        );

        ExcelDatasetWriter.toFile(file.toString())
            .cellWriter("name", ctx -> {
                ctx.getCell().setCellValue("CUSTOM_" + ctx.getValue());
                ctx.getCell().setCellStyle(ctx.getColumnStyle());
            })
            .write(data);

        try (Workbook wb = new XSSFWorkbook(new FileInputStream(file.toFile()))) {
            Sheet sheet = wb.getSheetAt(0);
            Row row = sheet.getRow(1);
            assertEquals("CUSTOM_Gadget", row.getCell(0).getStringCellValue());
            // Other fields should be default
            assertEquals(19.99, row.getCell(1).getNumericCellValue(), 0.001);
            assertEquals(10.0, row.getCell(2).getNumericCellValue());
        }
    }

    @Test
    void perFieldWriter_takesPriorityOverGlobal() throws IOException {
        Path file = tempDir.resolve("priority.xlsx");
        Dataset<Item> data = Dataset.of(
            new Item("X", new BigDecimal("5"), 1)
        );

        ExcelDatasetWriter.toFile(file.toString())
            .cellWriter(ctx -> {
                // Global: prefix with "G_"
                if (ctx.getValue() instanceof String s) {
                    ctx.getCell().setCellValue("G_" + s);
                    ctx.getCell().setCellStyle(ctx.getColumnStyle());
                } else {
                    ctx.writeDefault();
                }
            })
            .cellWriter("name", ctx -> {
                // Per-field: prefix with "F_"
                ctx.getCell().setCellValue("F_" + ctx.getValue());
                ctx.getCell().setCellStyle(ctx.getColumnStyle());
            })
            .write(data);

        try (Workbook wb = new XSSFWorkbook(new FileInputStream(file.toFile()))) {
            Sheet sheet = wb.getSheetAt(0);
            Row row = sheet.getRow(1);
            // Per-field wins over global for "name"
            assertEquals("F_X", row.getCell(0).getStringCellValue());
        }
    }

    @Test
    void writeDefault_delegatesToBuiltInLogic() throws IOException {
        Path file = tempDir.resolve("fallback.xlsx");
        Dataset<Item> data = Dataset.of(
            new Item("Thing", new BigDecimal("42"), 7)
        );

        AtomicBoolean defaultCalled = new AtomicBoolean(false);

        ExcelDatasetWriter.toFile(file.toString())
            .cellWriter(ctx -> {
                defaultCalled.set(true);
                ctx.writeDefault();
            })
            .write(data);

        assertTrue(defaultCalled.get());

        // Verify data is written correctly via default path
        Dataset<Item> read = ExcelDatasetReader.fromFile(file.toString())
            .hasHeaders(true).read(Item.class);
        assertEquals("Thing", read.first().orElseThrow().name());
    }

    @Test
    void contextExposesCorrectMetadata() throws IOException {
        Path file = tempDir.resolve("context.xlsx");
        Dataset<Item> data = Dataset.of(
            new Item("Test", new BigDecimal("1"), 1)
        );

        AtomicReference<FieldMeta> capturedMeta = new AtomicReference<>();
        AtomicReference<Object> capturedValue = new AtomicReference<>();
        AtomicReference<Workbook> capturedWorkbook = new AtomicReference<>();
        AtomicReference<Cell> capturedCell = new AtomicReference<>();

        ExcelDatasetWriter.toFile(file.toString())
            .cellWriter("name", ctx -> {
                capturedMeta.set(ctx.getFieldMeta());
                capturedValue.set(ctx.getValue());
                capturedWorkbook.set(ctx.getWorkbook());
                capturedCell.set(ctx.getCell());
                assertNotNull(ctx.getColumnStyle());
                ctx.writeDefault();
            })
            .write(data);

        assertNotNull(capturedMeta.get());
        assertEquals("name", capturedMeta.get().getFieldName());
        assertEquals("Test", capturedValue.get());
        assertNotNull(capturedWorkbook.get());
        assertNotNull(capturedCell.get());
    }

    @Test
    void cellWriterForMultipleFields() throws IOException {
        Path file = tempDir.resolve("multi.xlsx");
        Dataset<Item> data = Dataset.of(
            new Item("Item", new BigDecimal("10"), 3)
        );

        ExcelDatasetWriter.toFile(file.toString())
            .cellWriter(ctx -> {
                // Make all numeric cells show as zero
                ctx.getCell().setCellValue(0.0);
                ctx.getCell().setCellStyle(ctx.getColumnStyle());
            }, "price", "quantity")
            .write(data);

        try (Workbook wb = new XSSFWorkbook(new FileInputStream(file.toFile()))) {
            Sheet sheet = wb.getSheetAt(0);
            Row row = sheet.getRow(1);
            assertEquals("Item", row.getCell(0).getStringCellValue()); // unaffected
            assertEquals(0.0, row.getCell(1).getNumericCellValue());
            assertEquals(0.0, row.getCell(2).getNumericCellValue());
        }
    }

    @Test
    void customWriter_leaveCellBlank() throws IOException {
        Path file = tempDir.resolve("blank.xlsx");
        Dataset<Item> data = Dataset.of(
            new Item("Skip", new BigDecimal("99"), 1)
        );

        ExcelDatasetWriter.toFile(file.toString())
            .cellWriter("price", ctx -> {
                // intentionally do nothing — cell stays blank
            })
            .write(data);

        try (Workbook wb = new XSSFWorkbook(new FileInputStream(file.toFile()))) {
            Sheet sheet = wb.getSheetAt(0);
            Row row = sheet.getRow(1);
            assertEquals("Skip", row.getCell(0).getStringCellValue());
            assertEquals(CellType.BLANK, row.getCell(1).getCellType());
        }
    }

    public record FormattedNumber(
        @DataColumn(name = "Amount", order = 1,
                    numberFormat = "#,##0.00",
                    writeAs = DataColumn.WriteAs.STRING)
        double amount
    ) {}

    @Test
    void defaultCellWriter_usesUSLocaleForNumberFormat_regardlessOfSystemLocale() throws IOException {
        Locale saved = Locale.getDefault();
        Locale.setDefault(Locale.GERMANY);
        try {
            Path file = tempDir.resolve("locale.xlsx");
            Dataset<FormattedNumber> data = Dataset.of(new FormattedNumber(1234.56));

            ExcelDatasetWriter.toFile(file.toString()).write(data);

            try (Workbook wb = new XSSFWorkbook(new FileInputStream(file.toFile()))) {
                Sheet sheet = wb.getSheetAt(0);
                Row row = sheet.getRow(1);
                // Locale.GERMANY would produce "1.234,56"; Locale.US produces "1,234.56"
                assertEquals("1,234.56", row.getCell(0).getStringCellValue());
            }
        } finally {
            Locale.setDefault(saved);
        }
    }
}
