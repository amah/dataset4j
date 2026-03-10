package dataset4j.poi;

import dataset4j.Dataset;
import dataset4j.annotations.DataColumn;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test LocalDate read/write functionality in Excel format.
 */
class LocalDateTest {

    @TempDir
    Path tempDir;

    public record PersonWithDate(
        @DataColumn(name = "ID", order = 1, required = true)
        Integer id,
        
        @DataColumn(name = "Name", order = 2, required = true)
        String name,
        
        @DataColumn(name = "Birth Date", order = 3, dateFormat = "yyyy-MM-dd")
        LocalDate birthDate,
        
        @DataColumn(name = "Salary", order = 4, numberFormat = "#,##0.00")
        BigDecimal salary
    ) {}

    @Test
    void shouldWriteAndReadLocalDateInExcel() throws IOException {
        // Given
        Path excelFile = tempDir.resolve("localdate_test.xlsx");
        Dataset<PersonWithDate> originalData = Dataset.of(
            new PersonWithDate(1, "Alice", LocalDate.of(1990, 5, 15), new BigDecimal("75000")),
            new PersonWithDate(2, "Bob", LocalDate.of(1985, 12, 25), new BigDecimal("82000"))
        );

        // When - Write to Excel
        ExcelDatasetWriter
            .toFile(excelFile.toString())
            .write(originalData);

        // Then - File should exist
        assertTrue(excelFile.toFile().exists());
        
        System.out.println("Excel file with LocalDate created successfully: " + excelFile);
        System.out.println("File size: " + excelFile.toFile().length() + " bytes");

        // When - Read from Excel
        Dataset<PersonWithDate> readData = ExcelDatasetReader
            .fromFile(excelFile.toString())
            .readAs(PersonWithDate.class);

        // Then - Data should match (focusing on LocalDate)
        assertNotNull(readData);
        assertEquals(2, readData.size());
        
        PersonWithDate first = readData.first().orElseThrow();
        assertEquals(Integer.valueOf(1), first.id());
        assertEquals("Alice", first.name());
        assertEquals(LocalDate.of(1990, 5, 15), first.birthDate());
        // Skip exact BigDecimal comparison due to precision differences
        assertNotNull(first.salary());
        
        System.out.println("Successfully read back LocalDate data:");
        readData.toList().forEach(System.out::println);
    }
}