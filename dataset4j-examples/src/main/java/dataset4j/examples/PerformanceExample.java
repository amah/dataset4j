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
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * Performance benchmarking example comparing different formats and compression algorithms.
 * Tests write performance, file sizes, and compression ratios.
 */
public class PerformanceExample {

    public record Sale(
        @DataColumn(name = "ID", order = 1, required = true)
        Integer id,
        
        @DataColumn(name = "Product", order = 2, required = true)
        String product,
        
        @DataColumn(name = "Customer", order = 3, required = true)
        String customer,
        
        @DataColumn(name = "Amount", order = 4, numberFormat = "$#,##0.00")
        BigDecimal amount,
        
        @DataColumn(name = "Quantity", order = 5)
        Integer quantity,
        
        @DataColumn(name = "Date", order = 6, dateFormat = "yyyy-MM-dd")
        LocalDate date,
        
        @DataColumn(name = "Region", order = 7)
        String region,
        
        @DataColumn(name = "Category", order = 8)
        String category
    ) {}

    private static final String[] PRODUCTS = {
        "Laptop Pro", "Wireless Mouse", "USB Cable", "Monitor Stand", "Keyboard",
        "Tablet", "Phone Case", "Charger", "Headphones", "Speaker"
    };
    
    private static final String[] CUSTOMERS = {
        "Acme Corp", "Tech Solutions", "Global Systems", "Innovation Labs", "Digital Works",
        "Smart Industries", "Future Tech", "Data Dynamics", "Cloud Nine", "Pixel Perfect"
    };
    
    private static final String[] REGIONS = {
        "North America", "Europe", "Asia Pacific", "Latin America", "Middle East"
    };
    
    private static final String[] CATEGORIES = {
        "Electronics", "Accessories", "Software", "Hardware", "Services"
    };

    public static void main(String[] args) throws IOException {
        System.out.println("=== Dataset4J Performance Benchmark ===\n");
        
        // Test different dataset sizes
        int[] datasetSizes = {1000, 5000, 10000};
        
        for (int size : datasetSizes) {
            System.out.println("Testing with " + size + " records:");
            benchmarkFormats(size);
            System.out.println();
        }
    }
    
    private static void benchmarkFormats(int recordCount) throws IOException {
        // Generate test data
        Dataset<Sale> sales = generateSales(recordCount);
        
        Path tempDir = Files.createTempDirectory("dataset4j-benchmark");
        
        // Test Excel
        benchmarkExcel(sales, tempDir);
        
        // Test Parquet with different compression
        benchmarkParquet(sales, tempDir, ParquetCompressionCodec.UNCOMPRESSED, "UNCOMPRESSED");
        benchmarkParquet(sales, tempDir, ParquetCompressionCodec.SNAPPY, "SNAPPY");
        benchmarkParquet(sales, tempDir, ParquetCompressionCodec.LZ4, "LZ4");
        benchmarkParquet(sales, tempDir, ParquetCompressionCodec.GZIP, "GZIP");
        
        // Calculate compression ratios
        long uncompressedSize = Files.size(tempDir.resolve("sales_uncompressed.parquet"));
        long snappySize = Files.size(tempDir.resolve("sales_snappy.parquet"));
        long lz4Size = Files.size(tempDir.resolve("sales_lz4.parquet"));
        long gzipSize = Files.size(tempDir.resolve("sales_gzip.parquet"));
        long excelSize = Files.size(tempDir.resolve("sales.xlsx"));
        
        System.out.println("Compression Analysis:");
        System.out.printf("  Excel:        %,8d bytes (baseline)%n", excelSize);
        System.out.printf("  Uncompressed: %,8d bytes (%.1f%% vs Excel)%n", 
            uncompressedSize, ((double) uncompressedSize / excelSize) * 100);
        System.out.printf("  SNAPPY:       %,8d bytes (%.1f%% reduction)%n", 
            snappySize, (1.0 - (double) snappySize / uncompressedSize) * 100);
        System.out.printf("  LZ4:          %,8d bytes (%.1f%% reduction)%n", 
            lz4Size, (1.0 - (double) lz4Size / uncompressedSize) * 100);
        System.out.printf("  GZIP:         %,8d bytes (%.1f%% reduction)%n", 
            gzipSize, (1.0 - (double) gzipSize / uncompressedSize) * 100);
    }
    
    private static void benchmarkExcel(Dataset<Sale> sales, Path tempDir) throws IOException {
        Path excelFile = tempDir.resolve("sales.xlsx");
        
        long startTime = System.nanoTime();
        ExcelDatasetWriter
            .toFile(excelFile.toString())
            .sheet("Sales")
            .headers(true)
            .write(sales);
        long writeTime = System.nanoTime() - startTime;
        
        long fileSize = Files.size(excelFile);
        double throughput = (double) sales.size() / (writeTime / 1_000_000_000.0);
        
        System.out.printf("  Excel:        %5.1fms | %,8d bytes | %,.0f records/sec%n",
            writeTime / 1_000_000.0, fileSize, throughput);
    }
    
    private static void benchmarkParquet(Dataset<Sale> sales, Path tempDir, 
                                       ParquetCompressionCodec codec, String codecName) throws IOException {
        Path parquetFile = tempDir.resolve("sales_" + codecName.toLowerCase() + ".parquet");
        
        long startTime = System.nanoTime();
        ParquetDatasetWriter
            .toFile(parquetFile.toString())
            .withCompression(codec)
            .withRowGroupSize(5000)
            .write(sales);
        long writeTime = System.nanoTime() - startTime;
        
        long fileSize = Files.size(parquetFile);
        double throughput = (double) sales.size() / (writeTime / 1_000_000_000.0);
        
        System.out.printf("  %-12s: %5.1fms | %,8d bytes | %,.0f records/sec%n",
            codecName, writeTime / 1_000_000.0, fileSize, throughput);
    }
    
    private static Dataset<Sale> generateSales(int count) {
        Random random = new Random(42); // Fixed seed for reproducible results
        
        List<Sale> sales = IntStream.range(1, count + 1)
            .mapToObj(i -> new Sale(
                i,
                PRODUCTS[random.nextInt(PRODUCTS.length)],
                CUSTOMERS[random.nextInt(CUSTOMERS.length)],
                new BigDecimal(String.valueOf(50 + random.nextInt(950))), // $50-$1000
                1 + random.nextInt(10), // 1-10 quantity
                LocalDate.of(2023, 1 + random.nextInt(12), 1 + random.nextInt(28)),
                REGIONS[random.nextInt(REGIONS.length)],
                CATEGORIES[random.nextInt(CATEGORIES.length)]
            ))
            .toList();
        
        return Dataset.of(sales);
    }
}