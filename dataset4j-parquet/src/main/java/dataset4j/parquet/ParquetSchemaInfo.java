package dataset4j.parquet;

import java.util.List;

/**
 * Information about Parquet file schema.
 * Custom implementation avoiding Hadoop dependencies.
 */
public class ParquetSchemaInfo {
    
    private final ParquetSchema schema;
    
    public ParquetSchemaInfo(ParquetSchema schema) {
        this.schema = schema;
    }
    
    /**
     * Get the schema.
     * @return Parquet schema
     */
    public ParquetSchema getSchema() {
        return schema;
    }
    
    /**
     * Get column names in the schema.
     * @return list of column names
     */
    public List<String> getColumnNames() {
        return schema.getColumnNames();
    }
    
    /**
     * Get column count.
     * @return number of columns
     */
    public int getColumnCount() {
        return schema.getColumnCount();
    }
    
    /**
     * Get column by name.
     * @param name column name
     * @return column information or null if not found
     */
    public ParquetColumn getColumn(String name) {
        return schema.getColumn(name);
    }
    
    /**
     * Check if schema has column with given name.
     * @param name column name
     * @return true if column exists
     */
    public boolean hasColumn(String name) {
        return schema.hasColumn(name);
    }
    
    /**
     * Get all columns.
     * @return list of all columns
     */
    public List<ParquetColumn> getColumns() {
        return schema.getColumns();
    }
    
    /**
     * Get schema as string.
     * @return string representation
     */
    @Override
    public String toString() {
        return schema.toString();
    }
}