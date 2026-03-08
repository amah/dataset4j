package dataset4j.parquet;

import java.util.List;

/**
 * Metadata for a Parquet file.
 */
public class ParquetFileMetadata {
    
    private final ParquetSchema schema;
    private final List<ParquetRowGroup> rowGroups;
    private final long numRows;
    
    public ParquetFileMetadata(ParquetSchema schema, List<ParquetRowGroup> rowGroups) {
        this.schema = schema;
        this.rowGroups = rowGroups;
        this.numRows = rowGroups.stream()
            .mapToLong(ParquetRowGroup::getNumRows)
            .sum();
    }
    
    /** @return file schema */
    public ParquetSchema getSchema() {
        return schema;
    }
    
    /** @return list of row groups */
    public List<ParquetRowGroup> getRowGroups() {
        return rowGroups;
    }
    
    /** @return total number of rows */
    public long getNumRows() {
        return numRows;
    }
    
    /** @return number of row groups */
    public int getNumRowGroups() {
        return rowGroups.size();
    }
}