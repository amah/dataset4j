/**
 * Lightweight Parquet support for dataset4j with minimal dependencies.
 * 
 * <h2>Features</h2>
 * <ul>
 * <li>Custom Parquet implementation avoiding heavy Hadoop dependencies</li>
 * <li>Support for SNAPPY, GZIP, LZ4 compression</li>
 * <li>Annotation-driven schema mapping</li>
 * <li>High-performance columnar storage</li>
 * <li>Integration with dataset4j core API</li>
 * </ul>
 * 
 * <h2>Dependencies</h2>
 * <ul>
 * <li>dataset4j-core - Core functionality</li>
 * <li>parquet-format-structures - Parquet metadata structures</li>
 * <li>snappy-java - SNAPPY compression</li>
 * <li>lz4-java - LZ4 compression</li>
 * <li>libthrift - Thrift serialization</li>
 * </ul>
 * 
 * <h2>Usage Examples</h2>
 * 
 * <h3>Writing Parquet Files</h3>
 * <pre>{@code
 * Dataset<Employee> employees = Dataset.of(employeeList);
 * 
 * ParquetDatasetWriter
 *     .toFile("employees.parquet")
 *     .withCompression(ParquetCompressionCodec.SNAPPY)
 *     .withRowGroupSize(100000)
 *     .withMetadata("created_by", "dataset4j")
 *     .write(employees);
 * }</pre>
 * 
 * <h3>Reading Parquet Files</h3>
 * <pre>{@code
 * Dataset<Employee> employees = ParquetDatasetReader
 *     .fromFile("employees.parquet")
 *     .withCompression(ParquetCompressionCodec.SNAPPY)
 *     .readAs(Employee.class);
 * }</pre>
 * 
 * <h3>Schema Inspection</h3>
 * <pre>{@code
 * ParquetSchemaInfo schema = ParquetDatasetReader
 *     .fromFile("data.parquet")
 *     .getSchemaInfo();
 * 
 * System.out.println("Columns: " + schema.getColumnNames());
 * System.out.println("Column count: " + schema.getColumnCount());
 * }</pre>
 * 
 * <h3>Complete ETL Workflow</h3>
 * <pre>{@code
 * // Read from Excel
 * Dataset<Employee> data = DatasetIO.excel()
 *     .fromFile("input.xlsx")
 *     .readAs(Employee.class);
 * 
 * // Process data
 * Dataset<Employee> processed = data
 *     .filter(emp -> emp.salary() > 50000)
 *     .sortBy(Employee::department);
 * 
 * // Write to Parquet for analytics
 * DatasetIO.parquet()
 *     .toFile("warehouse/employees.parquet")
 *     .withCompression(ParquetCompressionCodec.SNAPPY)
 *     .withRowGroupSize(50000)
 *     .write(processed);
 * }</pre>
 * 
 * <h2>Performance Characteristics</h2>
 * <ul>
 * <li><strong>Compression:</strong> 70-90% size reduction with SNAPPY</li>
 * <li><strong>Row Groups:</strong> Optimal size 50K-100K rows for analytics</li>
 * <li><strong>Columnar Storage:</strong> Excellent for analytical queries</li>
 * <li><strong>Memory Efficiency:</strong> Streaming read/write operations</li>
 * </ul>
 * 
 * <h2>Supported Data Types</h2>
 * <ul>
 * <li>Primitives: boolean, int, long, float, double</li>
 * <li>Strings: String, char[]</li>
 * <li>Temporal: LocalDate, LocalDateTime (as strings)</li>
 * <li>Binary: byte[] (as BYTE_ARRAY)</li>
 * </ul>
 * 
 * @since 1.0.0
 */
package dataset4j.parquet;