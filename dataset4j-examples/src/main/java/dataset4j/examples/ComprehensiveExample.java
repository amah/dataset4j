package dataset4j.examples;

import dataset4j.Dataset;
import dataset4j.annotations.DataColumn;
import dataset4j.poi.ExcelDatasetReader;
import dataset4j.poi.ExcelDatasetWriter;
import dataset4j.parquet.ParquetCompressionCodec;
import dataset4j.parquet.ParquetDatasetReader;
import dataset4j.parquet.ParquetDatasetWriter;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Comprehensive example demonstrating Dataset4J capabilities across all supported formats.
 * Shows Excel, Parquet, and CSV integration with various data types and operations.
 */
public class ComprehensiveExample {

    public record Employee(
        @DataColumn(name = "ID", order = 1, required = true)
        Integer id,
        
        @DataColumn(name = "Name", order = 2, required = true)
        String name,
        
        @DataColumn(name = "Email", order = 3)
        String email,
        
        @DataColumn(name = "Active", order = 4)
        Boolean active,
        
        @DataColumn(name = "Salary", order = 5, numberFormat = "$#,##0.00")
        BigDecimal salary,
        
        @DataColumn(name = "Birth Date", order = 6, dateFormat = "yyyy-MM-dd")
        LocalDate birthDate,
        
        @DataColumn(name = "Department", order = 7)
        String department
    ) {}

    public static void main(String[] args) throws IOException {
        System.out.println("=== Dataset4J Comprehensive Example ===\n");
        
        // Create sample data
        Dataset<Employee> employees = createSampleData();
        System.out.println("Created dataset with " + employees.size() + " employees\n");
        
        // Create temp directory for outputs
        Path tempDir = Files.createTempDirectory("dataset4j-examples");
        System.out.println("Output directory: " + tempDir + "\n");
        
        // Demonstrate Excel operations
        demonstrateExcelOperations(employees, tempDir);
        
        // Demonstrate Parquet operations
        demonstrateParquetOperations(employees, tempDir);
        
        // Demonstrate data transformations
        demonstrateTransformations(employees);
        
        // Cleanup
        System.out.println("\nExample completed. Check output files in: " + tempDir);
    }
    
    private static Dataset<Employee> createSampleData() {
        List<Employee> employees = IntStream.range(1, 101)
            .mapToObj(i -> new Employee(
                i,
                "Employee " + i,
                "employee" + i + "@company.com",
                i % 3 != 0, // 2/3 active
                new BigDecimal(45000 + (i * 1000)), // Salary range 46k-145k
                LocalDate.of(1980 + (i % 30), ((i % 12) + 1), ((i % 28) + 1)),
                getDepartment(i)
            ))
            .toList();
        
        return Dataset.of(employees);
    }
    
    private static String getDepartment(int i) {
        return switch (i % 5) {
            case 0 -> "Engineering";
            case 1 -> "Sales";
            case 2 -> "Marketing";
            case 3 -> "HR";
            case 4 -> "Finance";
            default -> "Other";
        };
    }
    
    private static void demonstrateExcelOperations(Dataset<Employee> employees, Path tempDir) throws IOException {
        System.out.println("=== Excel Operations ===");
        
        Path excelFile = tempDir.resolve("employees.xlsx");
        
        // Write to Excel
        long startTime = System.currentTimeMillis();
        ExcelDatasetWriter
            .toFile(excelFile.toString())
            .sheet("Employees")
            .headers(true)
            .write(employees);
        long writeTime = System.currentTimeMillis() - startTime;
        
        System.out.println("✓ Excel file written in " + writeTime + "ms");
        System.out.println("  File size: " + Files.size(excelFile) + " bytes");
        
        // Read from Excel
        startTime = System.currentTimeMillis();
        Dataset<Employee> readEmployees = ExcelDatasetReader
            .fromFile(excelFile.toString())
            .headers(true)
            .readAs(Employee.class);
        long readTime = System.currentTimeMillis() - startTime;
        
        System.out.println("✓ Excel file read in " + readTime + "ms");
        System.out.println("  Records read: " + readEmployees.size());
        System.out.println("  Round-trip successful: " + (employees.size() == readEmployees.size()));
        System.out.println();
    }
    
    private static void demonstrateParquetOperations(Dataset<Employee> employees, Path tempDir) throws IOException {
        System.out.println("=== Parquet Operations ===");
        
        // Test different compression formats
        testParquetCompression(employees, tempDir, ParquetCompressionCodec.SNAPPY, "SNAPPY");
        testParquetCompression(employees, tempDir, ParquetCompressionCodec.GZIP, "GZIP");
        testParquetCompression(employees, tempDir, ParquetCompressionCodec.LZ4, "LZ4");
        testParquetCompression(employees, tempDir, ParquetCompressionCodec.UNCOMPRESSED, "UNCOMPRESSED");
        
        System.out.println();
    }
    
    private static void testParquetCompression(Dataset<Employee> employees, Path tempDir, 
                                             ParquetCompressionCodec codec, String codecName) throws IOException {
        
        Path parquetFile = tempDir.resolve("employees_" + codecName.toLowerCase() + ".parquet");
        
        // Write
        long startTime = System.currentTimeMillis();
        ParquetDatasetWriter
            .toFile(parquetFile.toString())
            .withCompression(codec)
            .withRowGroupSize(10000)
            .write(employees);
        long writeTime = System.currentTimeMillis() - startTime;
        
        long fileSize = Files.size(parquetFile);
        System.out.printf("✓ %-12s: %5d bytes (%3dms write)", 
            codecName, fileSize, writeTime);
        
        // Read
        startTime = System.currentTimeMillis();
        Dataset<Employee> readEmployees = ParquetDatasetReader
            .fromFile(parquetFile.toString())
            .readAs(Employee.class);
        long readTime = System.currentTimeMillis() - startTime;
        
        System.out.printf(" | %3dms read | %d records%n", 
            readTime, readEmployees.size());
    }
    
    private static void demonstrateTransformations(Dataset<Employee> employees) {
        System.out.println("=== Data Transformations ===");
        
        // Filter active employees
        Dataset<Employee> activeEmployees = employees.filter(emp -> emp.active());
        System.out.println("Active employees: " + activeEmployees.size() + "/" + employees.size());
        
        // Group by department (simplified)
        var departmentCounts = employees.toList().stream()
            .collect(java.util.stream.Collectors.groupingBy(
                Employee::department,
                java.util.stream.Collectors.counting()
            ));
        
        System.out.println("\nEmployees by department:");
        departmentCounts.forEach((dept, count) -> 
            System.out.println("  " + dept + ": " + count));
        
        // Calculate salary statistics
        var salaries = employees.toList().stream()
            .map(Employee::salary)
            .mapToDouble(BigDecimal::doubleValue);
        
        var stats = salaries.summaryStatistics();
        
        System.out.println("\nSalary statistics:");
        System.out.printf("  Average: $%,.2f%n", stats.getAverage());
        System.out.printf("  Maximum: $%,.2f%n", stats.getMax());
        System.out.printf("  Minimum: $%,.2f%n", stats.getMin());
        
        // Top 5 highest paid employees
        System.out.println("\nTop 5 highest paid employees:");
        employees.toList().stream()
            .sorted((a, b) -> b.salary().compareTo(a.salary())) // Sort descending
            .limit(5)
            .forEach(emp -> System.out.printf("  %s: $%s%n", emp.name(), emp.salary()));
    }
}