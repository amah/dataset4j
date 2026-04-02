package dataset4j.poi;

import dataset4j.Dataset;
import dataset4j.annotations.DataColumn;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ExcelMultiSheetTest {

    @TempDir
    Path tempDir;

    public record AScenario(
        @DataColumn(name = "ID", order = 1) int id,
        @DataColumn(name = "Label", order = 2) String label
    ) {}

    public record AResults(
        @DataColumn(name = "ID", order = 1) int id,
        @DataColumn(name = "Value", order = 2) double value
    ) {}

    public record BScenario(
        @DataColumn(name = "Code", order = 1) String code,
        @DataColumn(name = "Active", order = 2) boolean active
    ) {}

    @Test
    void shouldWriteMultipleSheetsToSingleFile() throws IOException {
        Path file = tempDir.resolve("multi_sheet.xlsx");

        Dataset<AScenario> aScenarios = Dataset.of(
            new AScenario(1, "Alpha"),
            new AScenario(2, "Beta")
        );
        Dataset<AResults> aResults = Dataset.of(
            new AResults(1, 3.14),
            new AResults(2, 2.71)
        );
        Dataset<BScenario> bScenarios = Dataset.of(
            new BScenario("X1", true),
            new BScenario("X2", false)
        );

        ExcelWorkbookWriter.toFile(file.toString())
            .addSheet("SheetAScenario", aScenarios)
            .addSheet("SheetA",         aResults)
            .addSheet("SheetBScenario", bScenarios)
            .write();

        assertTrue(file.toFile().exists());
        assertTrue(file.toFile().length() > 0);

        // Read back each sheet and verify row counts
        Dataset<AScenario> readAScenarios = ExcelDatasetReader.fromFile(file.toString())
            .sheet("SheetAScenario").hasHeaders(true).read(AScenario.class);
        assertEquals(2, readAScenarios.size());
        assertEquals(1, readAScenarios.first().orElseThrow().id());
        assertEquals("Alpha", readAScenarios.first().orElseThrow().label());

        Dataset<AResults> readAResults = ExcelDatasetReader.fromFile(file.toString())
            .sheet("SheetA").hasHeaders(true).read(AResults.class);
        assertEquals(2, readAResults.size());
        assertEquals(1, readAResults.first().orElseThrow().id());

        Dataset<BScenario> readBScenarios = ExcelDatasetReader.fromFile(file.toString())
            .sheet("SheetBScenario").hasHeaders(true).read(BScenario.class);
        assertEquals(2, readBScenarios.size());
        assertEquals("X1", readBScenarios.first().orElseThrow().code());
    }

    @Test
    void shouldHandleEmptySheetInMultiSheetWorkbook() throws IOException {
        Path file = tempDir.resolve("multi_with_empty.xlsx");

        Dataset<AScenario> data = Dataset.of(new AScenario(1, "OnlyRow"));
        Dataset<AResults> empty = Dataset.empty();

        ExcelWorkbookWriter.toFile(file.toString())
            .addSheet("Data",  data)
            .addSheet("Empty", empty)
            .write();

        assertTrue(file.toFile().exists());

        Dataset<AScenario> read = ExcelDatasetReader.fromFile(file.toString())
            .sheet("Data").hasHeaders(true).read(AScenario.class);
        assertEquals(1, read.size());
    }

    @Test
    void shouldRespectExplicitFieldSelectionPerSheet() throws IOException {
        Path file = tempDir.resolve("field_selection.xlsx");

        Dataset<AScenario> data = Dataset.of(
            new AScenario(10, "Gamma"),
            new AScenario(20, "Delta")
        );

        // Only export the "id" field
        ExcelWorkbookWriter.toFile(file.toString())
            .addSheet("Scenarios", data, "id")
            .write();

        assertTrue(file.toFile().exists());
    }

    @Test
    void shouldThrowWhenNoSheetsAdded() {
        assertThrows(IllegalStateException.class, () ->
            ExcelWorkbookWriter.toFile(tempDir.resolve("empty.xlsx").toString()).write());
    }

    @Test
    void shouldThrowOnPathTraversal() {
        assertThrows(SecurityException.class, () ->
            ExcelWorkbookWriter.toFile("../evil.xlsx"));
    }
}
