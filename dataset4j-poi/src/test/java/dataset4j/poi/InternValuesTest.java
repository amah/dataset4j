package dataset4j.poi;

import dataset4j.CellValue;
import dataset4j.Dataset;
import dataset4j.Table;
import dataset4j.annotations.DataColumn;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class InternValuesTest {

    @TempDir
    Path tempDir;

    public record Row(
        @DataColumn(name = "ID", order = 1) int id,
        @DataColumn(name = "Status", order = 2) String status,
        @DataColumn(name = "Amount", order = 3) BigDecimal amount
    ) {}

    @Test
    void shouldShareStringInstancesWhenInternEnabled() throws IOException {
        Path file = writeRepeatedSheet("intern_strings.xlsx");

        Dataset<Row> ds = ExcelDatasetReader.fromFile(file.toString())
            .internValues(true)
            .readAs(Row.class);

        assertEquals(4, ds.size());
        // Same string content across rows → must be the exact same instance.
        assertSame(ds.get(0).status(), ds.get(1).status());
        assertSame(ds.get(0).status(), ds.get(3).status());
        // Different content → different instance.
        assertNotSame(ds.get(0).status(), ds.get(2).status());
    }

    @Test
    void shouldShareBigDecimalInstancesWhenInternEnabled() throws IOException {
        Path file = writeRepeatedSheet("intern_bd.xlsx");

        Dataset<Row> ds = ExcelDatasetReader.fromFile(file.toString())
            .internValues(true)
            .readAs(Row.class);

        // Rows 0 and 1 both have amount=100.00 → same BigDecimal instance.
        assertSame(ds.get(0).amount(), ds.get(1).amount());
    }

    @Test
    void shouldNotShareInstancesByDefault() throws IOException {
        Path file = writeRepeatedSheet("no_intern.xlsx");

        Dataset<Row> ds = ExcelDatasetReader.fromFile(file.toString())
            .readAs(Row.class);

        // BigDecimals are constructed per cell by parseBasicValue, so without the pool
        // each row gets a fresh instance even when the value is the same.
        assertEquals(ds.get(0).amount(), ds.get(1).amount());
        assertNotSame(ds.get(0).amount(), ds.get(1).amount());
    }

    @Test
    void shouldShareCellValueInstancesInTableMode() throws IOException {
        Path file = writeRepeatedSheet("intern_table.xlsx");

        Table t = ExcelDatasetReader.fromFile(file.toString())
            .internValues(true)
            .readTable();

        CellValue s0 = t.get(0).get("Status");
        CellValue s1 = t.get(1).get("Status");
        CellValue s3 = t.get(3).get("Status");

        assertSame(s0, s1, "Same (value,type,format) triple must reuse the CellValue record");
        assertSame(s0, s3);
    }

    /** Sheet with 4 rows: statuses {A, A, B, A}, amounts {100, 100, 200, 100}. */
    private Path writeRepeatedSheet(String filename) throws IOException {
        Path file = tempDir.resolve(filename);
        try (var wb = new XSSFWorkbook();
             var fos = new FileOutputStream(file.toFile())) {
            var sheet = wb.createSheet();
            var h = sheet.createRow(0);
            h.createCell(0).setCellValue("ID");
            h.createCell(1).setCellValue("Status");
            h.createCell(2).setCellValue("Amount");

            Object[][] data = {
                {1d, "Active",   100d},
                {2d, "Active",   100d},
                {3d, "Inactive", 200d},
                {4d, "Active",   100d},
            };
            for (int r = 0; r < data.length; r++) {
                var row = sheet.createRow(r + 1);
                row.createCell(0).setCellValue((double) data[r][0]);
                row.createCell(1).setCellValue((String) data[r][1]);
                row.createCell(2).setCellValue((double) data[r][2]);
            }
            wb.write(fos);
        }
        return file;
    }
}
