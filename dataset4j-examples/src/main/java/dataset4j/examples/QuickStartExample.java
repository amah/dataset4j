package dataset4j.examples;

import dataset4j.Dataset;
import dataset4j.annotations.DataColumn;
import dataset4j.poi.ExcelDatasetWriter;
import dataset4j.parquet.ParquetCompressionCodec;
import dataset4j.parquet.ParquetDatasetWriter;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;

/**
 * Quick start example showing basic Dataset4J usage.
 * Perfect for getting started with the library.
 */
public class QuickStartExample {

    public record Person(
        @DataColumn(name = "ID", order = 1, required = true)
        Integer id,
        
        @DataColumn(name = "Full Name", order = 2, required = true) 
        String name,
        
        @DataColumn(name = "Email Address", order = 3)
        String email,
        
        @DataColumn(name = "Salary", order = 4, numberFormat = "$#,##0.00")
        BigDecimal salary,
        
        @DataColumn(name = "Birth Date", order = 5, dateFormat = "yyyy-MM-dd")
        LocalDate birthDate
    ) {}

    public static void main(String[] args) throws IOException {
        System.out.println("=== Dataset4J Quick Start ===\n");
        
        // 1. Create a dataset from records
        Dataset<Person> people = Dataset.of(
            new Person(1, "John Doe", "john@example.com", 
                new BigDecimal("75000"), LocalDate.of(1990, 5, 15)),
            new Person(2, "Jane Smith", "jane@example.com", 
                new BigDecimal("82000"), LocalDate.of(1985, 10, 22)),
            new Person(3, "Bob Wilson", "bob@example.com", 
                new BigDecimal("68000"), LocalDate.of(1992, 3, 8))
        );
        
        System.out.println("Created dataset with " + people.size() + " people");
        
        // 2. Transform data
        Dataset<Person> highEarners = people
            .filter(p -> p.salary().compareTo(new BigDecimal("70000")) > 0)
            .sortBy(p -> p.salary().negate()); // Sort by salary descending
        
        System.out.println("High earners (>$70K): " + highEarners.size());
        highEarners.forEach(p -> System.out.println("  " + p.name() + ": $" + p.salary()));
        
        // 3. Export to different formats
        Path tempDir = Files.createTempDirectory("dataset4j-quickstart");
        System.out.println("\nExporting to: " + tempDir);
        
        // Export to Excel
        ExcelDatasetWriter
            .toFile(tempDir.resolve("people.xlsx").toString())
            .sheet("People")
            .headers(true)
            .write(people);
        System.out.println("✓ Excel file created");
        
        // Export to Parquet with compression
        ParquetDatasetWriter
            .toFile(tempDir.resolve("people.parquet").toString())
            .withCompression(ParquetCompressionCodec.SNAPPY)
            .write(people);
        System.out.println("✓ Parquet file created");
        
        // Show file sizes
        long excelSize = Files.size(tempDir.resolve("people.xlsx"));
        long parquetSize = Files.size(tempDir.resolve("people.parquet"));
        
        System.out.printf("\nFile sizes:%n");
        System.out.printf("  Excel:   %,d bytes%n", excelSize);
        System.out.printf("  Parquet: %,d bytes%n", parquetSize);
        
        System.out.println("\nQuick start completed!");
    }
}