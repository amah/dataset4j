package dataset4j.poi;

import dataset4j.Dataset;
import dataset4j.annotations.DataColumn;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.FormulaError;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ExcelFormulaTest {

    @TempDir
    Path tempDir;

    public record Row2(
        @DataColumn(name = "A", order = 1) Integer a,
        @DataColumn(name = "B", order = 2) Integer b,
        @DataColumn(name = "Total", order = 3) Integer total
    ) {}

    /** Writes an .xlsx file with two formula columns: one that evaluates, one that errors. */
    private Path writeFixture(boolean withErrorRow) throws IOException {
        Path file = tempDir.resolve("formulas.xlsx");
        try (Workbook wb = new XSSFWorkbook()) {
            Sheet sheet = wb.createSheet("Sheet1");

            Row header = sheet.createRow(0);
            header.createCell(0).setCellValue("A");
            header.createCell(1).setCellValue("B");
            header.createCell(2).setCellValue("Total");

            // Normal formula row: Total = A + B, with cached result 30
            Row r1 = sheet.createRow(1);
            r1.createCell(0).setCellValue(10);
            r1.createCell(1).setCellValue(20);
            Cell sum = r1.createCell(2);
            sum.setCellFormula("A2+B2");
            // Prime the cached result so reading doesn't need an evaluator pass.
            wb.getCreationHelper().createFormulaEvaluator().evaluateFormulaCell(sum);

            if (withErrorRow) {
                // Error row: Total = A/B where B=0 → cached #DIV/0!
                Row r2 = sheet.createRow(2);
                r2.createCell(0).setCellValue(5);
                r2.createCell(1).setCellValue(0);
                Cell err = r2.createCell(2);
                err.setCellFormula("A3/B3");
                // Force the cached error state (cannot be evaluated to a number).
                err.setCellErrorValue(FormulaError.DIV0.getCode());
            }

            try (FileOutputStream fos = new FileOutputStream(file.toFile())) {
                wb.write(fos);
            }
        }
        return file;
    }

    @Test
    void readsCachedFormulaValueWithoutError() throws IOException {
        Path file = writeFixture(false);

        Dataset<Row2> data = ExcelDatasetReader.fromFile(file.toString()).readAs(Row2.class);

        assertEquals(1, data.size());
        Row2 only = data.toList().get(0);
        assertEquals(10, only.a());
        assertEquals(20, only.b());
        assertEquals(30, only.total(), "formula A+B should read from cached result");
    }

    @Test
    void defaultHandlerReturnsBlankForFormulaError() throws IOException {
        Path file = writeFixture(true);

        // Default handler = BLANK → error cell falls back to the field's default (null for boxed Integer).
        Dataset<Row2> data = ExcelDatasetReader.fromFile(file.toString()).readAs(Row2.class);

        assertEquals(2, data.size());
        Row2 errorRow = data.toList().get(1);
        assertEquals(5, errorRow.a());
        assertEquals(0, errorRow.b());
        assertNull(errorRow.total(), "#DIV/0! should be treated as blank by default");
    }

    @Test
    void customHandlerCanSubstituteValue() throws IOException {
        Path file = writeFixture(true);
        AtomicInteger invocations = new AtomicInteger();

        Dataset<Row2> data = ExcelDatasetReader
            .fromFile(file.toString())
            .onFormulaError((cell, error) -> {
                invocations.incrementAndGet();
                return "-1";
            })
            .readAs(Row2.class);

        assertEquals(1, invocations.get(), "handler should fire exactly once for the single error row");
        assertEquals(-1, data.toList().get(1).total());
    }
}
