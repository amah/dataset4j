package dataset4j.parquet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Lightweight representation of Parquet schema.
 * Custom implementation avoiding Hadoop dependencies.
 */
public class ParquetSchema {
    
    private final List<ParquetColumn> columns;
    private final Map<String, ParquetColumn> columnMap;
    
    public ParquetSchema() {
        this.columns = new ArrayList<>();
        this.columnMap = new HashMap<>();
    }
    
    public ParquetSchema(List<ParquetColumn> columns) {
        this.columns = new ArrayList<>(columns);
        this.columnMap = new HashMap<>();
        for (ParquetColumn column : columns) {
            columnMap.put(column.getName(), column);
        }
    }
    
    /**
     * Add column to schema.
     * @param column column definition
     */
    public void addColumn(ParquetColumn column) {
        columns.add(column);
        columnMap.put(column.getName(), column);
    }
    
    /**
     * Get all columns in schema.
     * @return list of columns
     */
    public List<ParquetColumn> getColumns() {
        return new ArrayList<>(columns);
    }
    
    /**
     * Get column by name.
     * @param name column name
     * @return column or null if not found
     */
    public ParquetColumn getColumn(String name) {
        return columnMap.get(name);
    }
    
    /**
     * Check if schema has column with given name.
     * @param name column name
     * @return true if column exists
     */
    public boolean hasColumn(String name) {
        return columnMap.containsKey(name);
    }
    
    /**
     * Get number of columns.
     * @return column count
     */
    public int getColumnCount() {
        return columns.size();
    }
    
    /**
     * Get column names.
     * @return list of column names
     */
    public List<String> getColumnNames() {
        return columns.stream()
            .map(ParquetColumn::getName)
            .toList();
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ParquetSchema{");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(columns.get(i));
        }
        sb.append("}");
        return sb.toString();
    }
}