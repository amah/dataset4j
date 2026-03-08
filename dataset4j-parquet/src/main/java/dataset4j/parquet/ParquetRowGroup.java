package dataset4j.parquet;

import java.util.List;

/**
 * Represents a row group in Parquet file.
 */
public class ParquetRowGroup {
    
    private final List<ParquetColumnChunk> columnChunks;
    private final long numRows;
    private final long totalByteSize;
    
    public ParquetRowGroup(List<ParquetColumnChunk> columnChunks, long numRows, long totalByteSize) {
        this.columnChunks = columnChunks;
        this.numRows = numRows;
        this.totalByteSize = totalByteSize;
    }
    
    /** @return list of column chunks */
    public List<ParquetColumnChunk> getColumnChunks() {
        return columnChunks;
    }
    
    /** @return number of rows in this row group */
    public long getNumRows() {
        return numRows;
    }
    
    /** @return total byte size of this row group */
    public long getTotalByteSize() {
        return totalByteSize;
    }
}