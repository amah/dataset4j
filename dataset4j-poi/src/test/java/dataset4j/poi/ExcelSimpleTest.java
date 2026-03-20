package dataset4j.poi;

import dataset4j.Dataset;
import dataset4j.annotations.DataColumn;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple Excel functionality tests without using generated field constants.
 */
class ExcelSimpleTest {

    @TempDir
    Path tempDir;

    public record SimpleEmployee(
        @DataColumn(name = "ID", order = 1, required = true)
        Integer id,
        
        @DataColumn(name = "Name", order = 2, required = true)
        String name,
        
        @DataColumn(name = "Email", order = 3)
        String email,
        
        @DataColumn(name = "Salary", order = 4, numberFormat = "$#,##0.00")
        BigDecimal salary
    ) {}

    @Test
    void shouldWriteAndReadBasicData() throws IOException {
        // Given
        Path excelFile = tempDir.resolve("simple_test.xlsx");
        Dataset<SimpleEmployee> originalData = Dataset.of(
            new SimpleEmployee(1, "John Doe", "john@company.com", new BigDecimal("75000.50")),
            new SimpleEmployee(2, "Jane Smith", "jane@company.com", new BigDecimal("82000.00"))
        );

        // When - Write to Excel
        ExcelDatasetWriter
            .toFile(excelFile.toString())
            .write(originalData);

        // Then - File should exist
        assertTrue(excelFile.toFile().exists());
        assertTrue(excelFile.toFile().length() > 0);
        
        System.out.println("Excel file created successfully: " + excelFile);
        System.out.println("File size: " + excelFile.toFile().length() + " bytes");

        // When - Read from Excel
        Dataset<SimpleEmployee> readData = ExcelDatasetReader
            .fromFile(excelFile.toString())
            .hasHeaders(true)
            .read(SimpleEmployee.class);

        // Then - Data should match
        assertNotNull(readData);
        assertEquals(2, readData.size());
        
        SimpleEmployee first = readData.first().orElseThrow();
        assertEquals(Integer.valueOf(1), first.id());
        assertEquals("John Doe", first.name());
        assertEquals("john@company.com", first.email());
        assertEquals(0, new BigDecimal("75000.50").compareTo(first.salary()));
        
        System.out.println("Successfully read back data:");
        readData.toList().forEach(System.out::println);
    }

    @Test
    void shouldHandleNullValues() throws IOException {
        // Given
        Path excelFile = tempDir.resolve("null_test.xlsx");
        Dataset<SimpleEmployee> dataWithNulls = Dataset.of(
            new SimpleEmployee(1, "John", null, null),
            new SimpleEmployee(2, "Jane", "jane@company.com", new BigDecimal("50000"))
        );

        // When - Write and read
        ExcelDatasetWriter
            .toFile(excelFile.toString())
            .write(dataWithNulls);

        Dataset<SimpleEmployee> readData = ExcelDatasetReader
            .fromFile(excelFile.toString())
            .hasHeaders(true)
            .read(SimpleEmployee.class);

        // Then - Should handle gracefully
        assertNotNull(readData);
        assertEquals(2, readData.size());
        
        SimpleEmployee withNulls = readData.first().orElseThrow();
        assertEquals(Integer.valueOf(1), withNulls.id());
        assertEquals("John", withNulls.name());
        // Note: null handling depends on implementation
    }

    public record EventRecord(
        @DataColumn(name = "Name", order = 1)
        String name,
        @DataColumn(name = "StartTime", order = 2)
        LocalDateTime startTime
    ) {}

    @Test
    void shouldPreserveTimeWhenReadingLocalDateTime() throws IOException {
        // Given
        Path excelFile = tempDir.resolve("datetime_test.xlsx");
        LocalDateTime expected = LocalDateTime.of(2024, 6, 15, 14, 30, 0);
        Dataset<EventRecord> original = Dataset.of(
            new EventRecord("Meeting", expected)
        );

        ExcelDatasetWriter.toFile(excelFile.toString()).write(original);

        // When
        Dataset<EventRecord> readData = ExcelDatasetReader
            .fromFile(excelFile.toString())
            .hasHeaders(true)
            .read(EventRecord.class);

        // Then
        LocalDateTime actual = readData.first().orElseThrow().startTime();
        assertEquals(expected, actual, "Time component must not be stripped during Excel read");
    }

    @Test
    void shouldCreateEmptyFile() throws IOException {
        // Given
        Path excelFile = tempDir.resolve("empty_test.xlsx");
        Dataset<SimpleEmployee> emptyData = Dataset.empty();

        // When - Write empty dataset
        ExcelDatasetWriter
            .toFile(excelFile.toString())
            .write(emptyData);

        // Then - Should create file
        assertTrue(excelFile.toFile().exists());
        System.out.println("Empty Excel file created: " + excelFile + " (size: " + excelFile.toFile().length() + " bytes)");
    }
}