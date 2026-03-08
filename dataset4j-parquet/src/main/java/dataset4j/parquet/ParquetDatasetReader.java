package dataset4j.parquet;

import dataset4j.Dataset;
import dataset4j.annotations.AnnotationProcessor;
import dataset4j.annotations.ColumnMetadata;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Lightweight Parquet reader with minimal dependencies.
 * Custom implementation avoiding heavy Hadoop dependencies.
 * 
 * Example usage:
 * {@code
 * Dataset<Employee> employees = ParquetDatasetReader
 *     .fromFile("employees.parquet")
 *     .readAs(Employee.class);
 * }
 */
public class ParquetDatasetReader {
    
    private final Path filePath;
    private ParquetCompressionCodec compressionCodec = ParquetCompressionCodec.UNCOMPRESSED;
    
    private ParquetDatasetReader(String filePath) {
        this.filePath = Paths.get(filePath);
    }
    
    /**
     * Create reader for Parquet file.
     * @param filePath path to Parquet file
     * @return new reader instance
     */
    public static ParquetDatasetReader fromFile(String filePath) {
        // Validate file path to prevent directory traversal attacks
        Path path = Paths.get(filePath).normalize();
        if (path.toString().contains("..")) {
            throw new SecurityException("Path traversal detected in file path: " + filePath);
        }
        
        // Check file size to prevent resource exhaustion attacks
        try {
            long fileSize = Files.size(path);
            if (fileSize > 100 * 1024 * 1024) { // 100MB limit
                throw new IOException("File too large: " + fileSize + " bytes (max 100MB)");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to check file size: " + e.getMessage(), e);
        }
        
        return new ParquetDatasetReader(filePath);
    }
    
    /**
     * Set compression codec for reading.
     * @param codec compression codec
     * @return this reader for chaining
     */
    public ParquetDatasetReader withCompression(ParquetCompressionCodec codec) {
        this.compressionCodec = codec;
        return this;
    }
    
    /**
     * Read Parquet data into Dataset of specified record type.
     * @param <T> record type
     * @param recordClass record class with @DataColumn annotations
     * @return Dataset containing parsed records
     * @throws IOException if file cannot be read
     */
    public <T> Dataset<T> readAs(Class<T> recordClass) throws IOException {
        if (!recordClass.isRecord()) {
            throw new IllegalArgumentException("Class must be a record: " + recordClass.getName());
        }
        
        List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(recordClass);
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("Record must have @DataColumn annotations");
        }
        
        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r");
             FileChannel channel = raf.getChannel()) {
            
            // Read Parquet file structure
            ParquetFileMetadata metadata = readFileMetadata(channel);
            ParquetSchema schema = metadata.getSchema();
            
            // Validate schema compatibility
            validateSchemaCompatibility(schema, columns);
            
            // Read row groups and parse data
            List<T> records = new ArrayList<>();
            for (ParquetRowGroup rowGroup : metadata.getRowGroups()) {
                List<T> rowGroupRecords = readRowGroup(channel, rowGroup, schema, recordClass, columns);
                records.addAll(rowGroupRecords);
            }
            
            return Dataset.of(records);
        }
    }
    
    /**
     * Get schema information from Parquet file.
     * @return schema information
     * @throws IOException if file cannot be read
     */
    public ParquetSchemaInfo getSchemaInfo() throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r");
             FileChannel channel = raf.getChannel()) {
            
            ParquetFileMetadata metadata = readFileMetadata(channel);
            return new ParquetSchemaInfo(metadata.getSchema());
        }
    }
    
    /**
     * Check if file exists and is readable.
     * @return true if file can be read
     */
    public boolean canRead() {
        try {
            return filePath.toFile().exists() && filePath.toFile().canRead();
        } catch (Exception e) {
            return false;
        }
    }
    
    // Private implementation methods
    
    private ParquetFileMetadata readFileMetadata(FileChannel channel) throws IOException {
        // Read footer length (last 4 bytes)
        long fileSize = channel.size();
        ByteBuffer footerLengthBuffer = ByteBuffer.allocate(4);
        channel.position(fileSize - 8); // Skip magic number
        channel.read(footerLengthBuffer);
        footerLengthBuffer.flip();
        int footerLength = footerLengthBuffer.getInt();
        
        // Read footer
        ByteBuffer footerBuffer = ByteBuffer.allocate(footerLength);
        channel.position(fileSize - 8 - footerLength);
        channel.read(footerBuffer);
        footerBuffer.flip();
        
        // Parse footer using Thrift
        return parseFooter(footerBuffer);
    }
    
    private ParquetFileMetadata parseFooter(ByteBuffer footerBuffer) throws IOException {
        // This is a simplified implementation
        // In a real implementation, you would use Thrift to parse the footer
        // For now, we'll create a mock implementation
        
        ParquetSchema schema = new ParquetSchema();
        List<ParquetRowGroup> rowGroups = new ArrayList<>();
        
        // Parse actual footer structure here...
        // This would involve Thrift deserialization
        
        return new ParquetFileMetadata(schema, rowGroups);
    }
    
    private void validateSchemaCompatibility(ParquetSchema schema, List<ColumnMetadata> columns) {
        // Check if record fields match Parquet schema
        for (ColumnMetadata column : columns) {
            if (!schema.hasColumn(column.getFieldName())) {
                throw new IllegalArgumentException(
                    "Column not found in Parquet schema: " + column.getFieldName());
            }
        }
    }
    
    private <T> List<T> readRowGroup(FileChannel channel, ParquetRowGroup rowGroup, 
                                   ParquetSchema schema, Class<T> recordClass, 
                                   List<ColumnMetadata> columns) throws IOException {
        
        List<T> records = new ArrayList<>();
        
        // Read column chunks for this row group
        for (ParquetColumnChunk columnChunk : rowGroup.getColumnChunks()) {
            // Read and decompress column data
            ByteBuffer columnData = readColumnChunk(channel, columnChunk);
            
            // Parse column values
            // This would involve reading the column's encoded data
            // and converting it to Java objects
        }
        
        // Reconstruct records from column data
        // This is complex and would require implementing:
        // 1. Column encoding/decoding (PLAIN, DICTIONARY, etc.)
        // 2. Compression handling (SNAPPY, GZIP, LZ4, etc.)
        // 3. Type conversion from Parquet types to Java types
        // 4. Record reconstruction from columnar data
        
        // For now, return empty list as this is demonstration code
        return records;
    }
    
    private ByteBuffer readColumnChunk(FileChannel channel, ParquetColumnChunk columnChunk) 
            throws IOException {
        
        long offset = columnChunk.getFileOffset();
        int length = columnChunk.getCompressedSize();
        
        ByteBuffer buffer = ByteBuffer.allocate(length);
        channel.position(offset);
        channel.read(buffer);
        buffer.flip();
        
        // Decompress if needed
        if (columnChunk.getCompressionCodec() != ParquetCompressionCodec.UNCOMPRESSED) {
            return decompress(buffer, columnChunk.getCompressionCodec(), 
                           columnChunk.getUncompressedSize());
        }
        
        return buffer;
    }
    
    private ByteBuffer decompress(ByteBuffer compressed, ParquetCompressionCodec codec, 
                                int uncompressedSize) throws IOException {
        
        byte[] compressedBytes = new byte[compressed.remaining()];
        compressed.get(compressedBytes);
        
        byte[] uncompressed = switch (codec) {
            case SNAPPY -> {
                try {
                    yield org.xerial.snappy.Snappy.uncompress(compressedBytes);
                } catch (Exception e) {
                    throw new IOException("Failed to decompress SNAPPY data", e);
                }
            }
            case GZIP -> {
                try (java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(compressedBytes);
                     java.util.zip.GZIPInputStream gzipIn = new java.util.zip.GZIPInputStream(bais);
                     java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream()) {
                    
                    byte[] buffer = new byte[1024];
                    int len;
                    while ((len = gzipIn.read(buffer)) != -1) {
                        baos.write(buffer, 0, len);
                    }
                    yield baos.toByteArray();
                } catch (Exception e) {
                    throw new IOException("Failed to decompress GZIP data", e);
                }
            }
            case LZ4 -> {
                try {
                    net.jpountz.lz4.LZ4Factory factory = net.jpountz.lz4.LZ4Factory.fastestInstance();
                    net.jpountz.lz4.LZ4FastDecompressor decompressor = factory.fastDecompressor();
                    yield decompressor.decompress(compressedBytes, uncompressedSize);
                } catch (Exception e) {
                    throw new IOException("Failed to decompress LZ4 data", e);
                }
            }
            default -> throw new UnsupportedOperationException("Compression codec not supported: " + codec);
        };
        
        return ByteBuffer.wrap(uncompressed);
    }
}