package dataset4j.parquet;

import dataset4j.Dataset;
import dataset4j.annotations.DataColumn;
import dataset4j.annotations.FieldSelector;
import dataset4j.annotations.MetadataCache;
import dataset4j.annotations.PojoMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple Parquet functionality tests without using generated field constants.
 * Tests real Parquet file creation and reading with various data types.
 */
class ParquetSimpleTest {

    @TempDir
    Path tempDir;

    public record SimpleEmployee(
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
        LocalDate birthDate
    ) {}

    @Test
    void shouldWriteAndReadBasicDataWithSnappy() throws IOException {
        // Given
        Path parquetFile = tempDir.resolve("simple_test_snappy.parquet");
        Dataset<SimpleEmployee> originalData = Dataset.of(
            new SimpleEmployee(1, "John Doe", "john@company.com", true, 
                new BigDecimal("75000.50"), LocalDate.of(1990, 5, 15)),
            new SimpleEmployee(2, "Jane Smith", "jane@company.com", false, 
                new BigDecimal("82000.00"), LocalDate.of(1985, 10, 22)),
            new SimpleEmployee(3, "Bob Wilson", "bob@company.com", true, 
                new BigDecimal("65000.75"), LocalDate.of(1992, 2, 8))
        );

        // When - Write to Parquet with SNAPPY compression
        ParquetDatasetWriter
            .toFile(parquetFile.toString())
            .withCompression(ParquetCompressionCodec.SNAPPY)
            .write(originalData);

        // Then - File should exist
        assertTrue(parquetFile.toFile().exists());
        assertTrue(parquetFile.toFile().length() > 0);
        
        System.out.println("Parquet file (SNAPPY) created successfully: " + parquetFile);
        System.out.println("File size: " + parquetFile.toFile().length() + " bytes");

        // When - Read from Parquet
        Dataset<SimpleEmployee> readData = ParquetDatasetReader
            .fromFile(parquetFile.toString())
            .readAs(SimpleEmployee.class);

        // Then - Data should match
        assertNotNull(readData);
        assertEquals(3, readData.size());
        
        SimpleEmployee first = readData.first().orElseThrow();
        assertEquals(Integer.valueOf(1), first.id());
        assertEquals("John Doe", first.name());
        assertEquals("john@company.com", first.email());
        assertTrue(first.active());
        assertEquals(new BigDecimal("75000.50"), first.salary());
        assertEquals(LocalDate.of(1990, 5, 15), first.birthDate());
        
        System.out.println("Successfully read back data:");
        readData.toList().forEach(System.out::println);
    }

    @Test
    void shouldSupportLZ4Compression() throws IOException {
        Path parquetFile = tempDir.resolve("test_lz4.parquet");
        SimpleEmployee original = new SimpleEmployee(1, "Alice Brown", "alice@company.com", true,
                new BigDecimal("90000.25"), LocalDate.of(1988, 7, 3));
        ParquetDatasetWriter.toFile(parquetFile.toString())
                .withCompression(ParquetCompressionCodec.LZ4)
                .write(Dataset.of(original));

        Dataset<SimpleEmployee> readBack = ParquetDatasetReader
                .fromFile(parquetFile.toString())
                .readAs(SimpleEmployee.class);
        assertEquals(1, readBack.size());
        assertEquals(original, readBack.first().orElseThrow());
    }

    @Test
    void shouldSupportGZIPCompression() throws IOException {
        Path parquetFile = tempDir.resolve("test_gzip.parquet");
        SimpleEmployee original = new SimpleEmployee(1, "Charlie Davis", "charlie@company.com", true,
                new BigDecimal("95000.00"), LocalDate.of(1991, 12, 25));
        ParquetDatasetWriter.toFile(parquetFile.toString())
                .withCompression(ParquetCompressionCodec.GZIP)
                .write(Dataset.of(original));

        Dataset<SimpleEmployee> readBack = ParquetDatasetReader
                .fromFile(parquetFile.toString())
                .readAs(SimpleEmployee.class);
        assertEquals(1, readBack.size());
        assertEquals(original, readBack.first().orElseThrow());
    }

    @Test
    void shouldSupportUncompressedFormat() throws IOException {
        Path parquetFile = tempDir.resolve("test_uncompressed.parquet");
        SimpleEmployee original = new SimpleEmployee(1, "Diana Evans", "diana@company.com", false,
                new BigDecimal("87500.50"), LocalDate.of(1987, 4, 14));
        ParquetDatasetWriter.toFile(parquetFile.toString())
                .withCompression(ParquetCompressionCodec.UNCOMPRESSED)
                .write(Dataset.of(original));

        Dataset<SimpleEmployee> readBack = ParquetDatasetReader
                .fromFile(parquetFile.toString())
                .readAs(SimpleEmployee.class);
        assertEquals(1, readBack.size());
        assertEquals(original, readBack.first().orElseThrow());
    }

    @Test
    void shouldHandleNullValues() throws IOException {
        Path parquetFile = tempDir.resolve("null_values.parquet");
        SimpleEmployee withNulls = new SimpleEmployee(1, "John", null, null, null, null);
        SimpleEmployee complete = new SimpleEmployee(2, "Jane", "jane@company.com", true,
                new BigDecimal("50000"), LocalDate.of(2024, 1, 15));

        ParquetDatasetWriter.toFile(parquetFile.toString())
                .withCompression(ParquetCompressionCodec.SNAPPY)
                .write(Dataset.of(withNulls, complete));

        Dataset<SimpleEmployee> readBack = ParquetDatasetReader
                .fromFile(parquetFile.toString())
                .readAs(SimpleEmployee.class);
        assertEquals(2, readBack.size());
        assertEquals(withNulls, readBack.toList().get(0));
        assertEquals(complete, readBack.toList().get(1));
    }

    @Test
    void shouldCreateEmptyFile() throws IOException {
        // Given
        Path parquetFile = tempDir.resolve("empty.parquet");
        Dataset<SimpleEmployee> emptyData = Dataset.empty();

        // When - Write empty dataset
        try {
            ParquetDatasetWriter
                .toFile(parquetFile.toString())
                .withCompression(ParquetCompressionCodec.SNAPPY)
                .write(emptyData);
                
            System.out.println("Empty Parquet file created: " + parquetFile + 
                " (size: " + parquetFile.toFile().length() + " bytes)");
        } catch (IllegalArgumentException e) {
            // Some implementations don't allow empty datasets
            System.out.println("Empty dataset handling: " + e.getMessage());
        }
    }

    @Test
    void shouldHandleSpecialCharacters() throws IOException {
        Path parquetFile = tempDir.resolve("special_chars.parquet");
        SimpleEmployee original = new SimpleEmployee(
                1,
                "José María González-Pérez",
                "josé.maría@château.com",
                true,
                new BigDecimal("95500.50"),
                LocalDate.of(1990, 5, 15));

        ParquetDatasetWriter.toFile(parquetFile.toString())
                .withCompression(ParquetCompressionCodec.SNAPPY)
                .write(Dataset.of(original));

        Dataset<SimpleEmployee> readBack = ParquetDatasetReader
                .fromFile(parquetFile.toString())
                .readAs(SimpleEmployee.class);
        assertEquals(1, readBack.size());
        assertEquals(original, readBack.first().orElseThrow());
    }

    @Test
    void shouldSupportLargeDataset() throws IOException {
        // Given - Medium dataset (100 records)
        List<SimpleEmployee> records = java.util.stream.IntStream.range(1, 101)
            .mapToObj(i -> new SimpleEmployee(
                i,
                "Employee " + i,
                "employee" + i + "@company.com",
                i % 2 == 0,
                new BigDecimal(50000 + (i * 500)),
                LocalDate.of(1980 + (i % 30), ((i % 12) + 1), ((i % 28) + 1))
            ))
            .toList();
        Dataset<SimpleEmployee> largeData = Dataset.of(records);
        Path parquetFile = tempDir.resolve("large_dataset.parquet");

        // When - Write large dataset
        long startTime = System.currentTimeMillis();
        
        ParquetDatasetWriter
            .toFile(parquetFile.toString())
            .withCompression(ParquetCompressionCodec.SNAPPY)
            .write(largeData);
        
        long writeTime = System.currentTimeMillis() - startTime;

        // Then - Should handle efficiently
        assertTrue(writeTime < 5000, "Write should complete within 5 seconds");
        assertTrue(parquetFile.toFile().exists());

        Dataset<SimpleEmployee> readBack = ParquetDatasetReader
                .fromFile(parquetFile.toString())
                .readAs(SimpleEmployee.class);
        assertEquals(100, readBack.size());
        assertEquals(records.get(0), readBack.toList().get(0));
        assertEquals(records.get(99), readBack.toList().get(99));
    }

    @Test
    void shouldWriteSelectedFieldsOnly() throws IOException {
        // Given
        Path parquetFile = tempDir.resolve("selected_fields.parquet");
        Dataset<SimpleEmployee> data = Dataset.of(
            new SimpleEmployee(1, "John Doe", "john@company.com", true,
                new BigDecimal("75000.50"), LocalDate.of(1990, 5, 15)),
            new SimpleEmployee(2, "Jane Smith", "jane@company.com", false,
                new BigDecimal("82000.00"), LocalDate.of(1985, 10, 22))
        );

        PojoMetadata<SimpleEmployee> meta = MetadataCache.getMetadata(SimpleEmployee.class);

        // When - Write only id and name fields
        ParquetDatasetWriter
            .toFile(parquetFile.toString())
            .select(meta)
            .fields("id", "name")
            .withCompression(ParquetCompressionCodec.SNAPPY)
            .write(data);

        // Then - File should exist and be smaller than full export
        assertTrue(parquetFile.toFile().exists());
        assertTrue(parquetFile.toFile().length() > 0);

        // Write full dataset for size comparison
        Path fullFile = tempDir.resolve("full_fields.parquet");
        ParquetDatasetWriter
            .toFile(fullFile.toString())
            .withCompression(ParquetCompressionCodec.SNAPPY)
            .write(data);

        System.out.println("Selected fields file size: " + parquetFile.toFile().length() + " bytes");
        System.out.println("Full fields file size: " + fullFile.toFile().length() + " bytes");
        assertTrue(parquetFile.toFile().length() <= fullFile.toFile().length(),
            "Selected fields file should be smaller or equal to full file");
    }

    @Test
    void shouldWriteWithExcludedFields() throws IOException {
        // Given
        Path parquetFile = tempDir.resolve("excluded_fields.parquet");
        Dataset<SimpleEmployee> data = Dataset.of(
            new SimpleEmployee(1, "John Doe", "john@company.com", true,
                new BigDecimal("75000.50"), LocalDate.of(1990, 5, 15))
        );

        PojoMetadata<SimpleEmployee> meta = MetadataCache.getMetadata(SimpleEmployee.class);

        // When - Write excluding email and salary
        ParquetDatasetWriter
            .toFile(parquetFile.toString())
            .select(meta)
            .exclude("email", "salary")
            .withCompression(ParquetCompressionCodec.SNAPPY)
            .write(data);

        // Then
        assertTrue(parquetFile.toFile().exists());
        assertTrue(parquetFile.toFile().length() > 0);
    }

    @Test
    void shouldWriteWithFieldSelector() throws IOException {
        // Given
        Path parquetFile = tempDir.resolve("selector_fields.parquet");
        Dataset<SimpleEmployee> data = Dataset.of(
            new SimpleEmployee(1, "John Doe", "john@company.com", true,
                new BigDecimal("75000.50"), LocalDate.of(1990, 5, 15))
        );

        FieldSelector<SimpleEmployee> selector = FieldSelector.from(SimpleEmployee.class)
            .requiredOnly();

        // When - Write only required fields
        ParquetDatasetWriter
            .toFile(parquetFile.toString())
            .select(selector)
            .withCompression(ParquetCompressionCodec.SNAPPY)
            .write(data);

        // Then
        assertTrue(parquetFile.toFile().exists());
        assertTrue(parquetFile.toFile().length() > 0);
    }

    @Test
    void shouldRoundTripBigDecimalAsLogicalDecimal() throws IOException {
        Path parquetFile = tempDir.resolve("decimal_logical.parquet");
        Dataset<SimpleEmployee> data = Dataset.of(
            new SimpleEmployee(1, "Alice", "a@x.com", true,
                new BigDecimal("12345.67"), LocalDate.of(1990, 1, 1)),
            new SimpleEmployee(2, "Bob", "b@x.com", false,
                new BigDecimal("9876543.210"), LocalDate.of(1985, 6, 20)),
            new SimpleEmployee(3, "Carol", "c@x.com", true,
                new BigDecimal("0.5"), LocalDate.of(2001, 12, 31))
        );

        ParquetDatasetWriter.toFile(parquetFile.toString())
                .withCompression(ParquetCompressionCodec.SNAPPY)
                .withBigDecimalAsLogicalType(true)
                .write(data);

        Dataset<SimpleEmployee> readBack = ParquetDatasetReader
                .fromFile(parquetFile.toString())
                .readAs(SimpleEmployee.class);
        assertEquals(3, readBack.size());
        // All values are rescaled to the max scale (3) on write — comparison via compareTo
        // since BigDecimal.equals is scale-sensitive.
        assertEquals(0, new BigDecimal("12345.67").compareTo(readBack.toList().get(0).salary()));
        assertEquals(0, new BigDecimal("9876543.210").compareTo(readBack.toList().get(1).salary()));
        assertEquals(0, new BigDecimal("0.5").compareTo(readBack.toList().get(2).salary()));
    }

    @Test
    void shouldCompareCompressionEfficiency() throws IOException {
        // Given - Same dataset for all compression types
        Dataset<SimpleEmployee> data = Dataset.of(
            java.util.stream.IntStream.range(1, 51)  // 50 records
                .mapToObj(i -> new SimpleEmployee(
                    i,
                    "Employee " + i,
                    "employee" + i + "@company.com",
                    i % 2 == 0,
                    new BigDecimal(50000 + (i * 1000)),
                    LocalDate.of(1990, (i % 12) + 1, (i % 28) + 1)
                ))
                .toList()
        );

        // When - Test all compression formats
        Path uncompressedFile = tempDir.resolve("comparison_uncompressed.parquet");
        Path snappyFile = tempDir.resolve("comparison_snappy.parquet");
        Path lz4File = tempDir.resolve("comparison_lz4.parquet");
        Path gzipFile = tempDir.resolve("comparison_gzip.parquet");

        // Write with different compression
        ParquetDatasetWriter.toFile(uncompressedFile.toString())
            .withCompression(ParquetCompressionCodec.UNCOMPRESSED)
            .write(data);
            
        ParquetDatasetWriter.toFile(snappyFile.toString())
            .withCompression(ParquetCompressionCodec.SNAPPY)
            .write(data);
            
        ParquetDatasetWriter.toFile(lz4File.toString())
            .withCompression(ParquetCompressionCodec.LZ4)
            .write(data);
            
        ParquetDatasetWriter.toFile(gzipFile.toString())
            .withCompression(ParquetCompressionCodec.GZIP)
            .write(data);

        // Then - Compare file sizes
        long uncompressedSize = uncompressedFile.toFile().length();
        long snappySize = snappyFile.toFile().length();
        long lz4Size = lz4File.toFile().length();
        long gzipSize = gzipFile.toFile().length();

        System.out.println("\n=== Compression Comparison (50 records) ===");
        System.out.println("UNCOMPRESSED: " + uncompressedSize + " bytes");
        System.out.println("SNAPPY:       " + snappySize + " bytes (" + 
            String.format("%.1f%%", (1.0 - (double)snappySize / uncompressedSize) * 100) + " reduction)");
        System.out.println("LZ4:          " + lz4Size + " bytes (" + 
            String.format("%.1f%%", (1.0 - (double)lz4Size / uncompressedSize) * 100) + " reduction)");
        System.out.println("GZIP:         " + gzipSize + " bytes (" + 
            String.format("%.1f%%", (1.0 - (double)gzipSize / uncompressedSize) * 100) + " reduction)");

        // All files should exist
        assertTrue(uncompressedFile.toFile().exists());
        assertTrue(snappyFile.toFile().exists());
        assertTrue(lz4File.toFile().exists());
        assertTrue(gzipFile.toFile().exists());
    }
}