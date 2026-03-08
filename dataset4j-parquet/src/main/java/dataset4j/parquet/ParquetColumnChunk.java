package dataset4j.parquet;

/**
 * Represents a column chunk in a Parquet row group.
 */
public class ParquetColumnChunk {
    
    private final String columnPath;
    private final ParquetDataType dataType;
    private final long fileOffset;
    private final int compressedSize;
    private final int uncompressedSize;
    private final ParquetCompressionCodec compressionCodec;
    private final long numValues;
    
    public ParquetColumnChunk(String columnPath, ParquetDataType dataType, 
                             long fileOffset, int compressedSize, int uncompressedSize,
                             ParquetCompressionCodec compressionCodec, long numValues) {
        this.columnPath = columnPath;
        this.dataType = dataType;
        this.fileOffset = fileOffset;
        this.compressedSize = compressedSize;
        this.uncompressedSize = uncompressedSize;
        this.compressionCodec = compressionCodec;
        this.numValues = numValues;
    }
    
    /** @return column path */
    public String getColumnPath() {
        return columnPath;
    }
    
    /** @return data type */
    public ParquetDataType getDataType() {
        return dataType;
    }
    
    /** @return file offset where this chunk starts */
    public long getFileOffset() {
        return fileOffset;
    }
    
    /** @return compressed size in bytes */
    public int getCompressedSize() {
        return compressedSize;
    }
    
    /** @return uncompressed size in bytes */
    public int getUncompressedSize() {
        return uncompressedSize;
    }
    
    /** @return compression codec used */
    public ParquetCompressionCodec getCompressionCodec() {
        return compressionCodec;
    }
    
    /** @return number of values in this chunk */
    public long getNumValues() {
        return numValues;
    }
}