package dataset4j;

/**
 * Convenience class providing access to all dataset4j I/O capabilities.
 * 
 * This class serves as a single entry point for accessing all format-specific
 * readers and writers when using the complete dataset4j library.
 * 
 * Example usage:
 * {@code
 * // Read from Excel
 * Dataset<Employee> data = DatasetIO.excel()
 *     .fromFile("input.xlsx")
 *     .readAs(Employee.class);
 * 
 * // Process data
 * Dataset<Employee> filtered = data
 *     .filter(emp -> emp.department().equals("IT"))
 *     .sortBy(Employee::salary);
 * 
 * // Write to Parquet
 * DatasetIO.parquet()
 *     .toFile("output.parquet")
 *     .write(filtered);
 * }
 */
public final class DatasetIO {
    
    private DatasetIO() {
        // Utility class
    }
    
    /**
     * Access Excel I/O capabilities.
     * @return Excel I/O builder
     */
    public static ExcelIO excel() {
        return new ExcelIO();
    }
    
    /**
     * Access CSV I/O capabilities.
     * @return CSV I/O builder
     */
    public static CsvIO csv() {
        return new CsvIO();
    }
    
    /**
     * Access Parquet I/O capabilities.
     * @return Parquet I/O builder
     */
    public static ParquetIO parquet() {
        return new ParquetIO();
    }
    
    /**
     * Excel I/O operations.
     */
    public static class ExcelIO {
        
        /**
         * Create Excel reader.
         * @param filePath path to Excel file
         * @return Excel reader
         */
        public dataset4j.poi.ExcelDatasetReader fromFile(String filePath) {
            return dataset4j.poi.ExcelDatasetReader.fromFile(filePath);
        }
        
        /**
         * Create Excel writer.
         * @param filePath path to output Excel file
         * @return Excel writer
         */
        public dataset4j.poi.ExcelDatasetWriter toFile(String filePath) {
            return dataset4j.poi.ExcelDatasetWriter.toFile(filePath);
        }
    }
    
    /**
     * CSV I/O operations.
     */
    public static class CsvIO {

        /**
         * Create CSV reader for reading into an untyped {@link Table}.
         * @param filePath path to CSV file
         * @return CSV reader
         */
        public dataset4j.poi.CsvDatasetReader fromFile(String filePath) {
            return dataset4j.poi.CsvDatasetReader.fromFile(filePath);
        }

        /**
         * Create CSV writer.
         * @param filePath path to output CSV file
         * @return CSV writer
         */
        public dataset4j.poi.CsvDatasetWriter toFile(String filePath) {
            return dataset4j.poi.CsvDatasetWriter.toFile(filePath);
        }
    }
    
    /**
     * Parquet I/O operations.
     */
    public static class ParquetIO {
        
        /**
         * Create Parquet reader.
         * @param filePath path to Parquet file
         * @return Parquet reader
         */
        public dataset4j.parquet.ParquetDatasetReader fromFile(String filePath) {
            return dataset4j.parquet.ParquetDatasetReader.fromFile(filePath);
        }
        
        /**
         * Create Parquet writer.
         * @param filePath path to output Parquet file
         * @return Parquet writer
         */
        public dataset4j.parquet.ParquetDatasetWriter toFile(String filePath) {
            return dataset4j.parquet.ParquetDatasetWriter.toFile(filePath);
        }
    }
}