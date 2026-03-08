package dataset4j.parquet;

/**
 * Represents a column in Parquet schema.
 */
public class ParquetColumn {
    
    private final String name;
    private final ParquetDataType dataType;
    private final boolean required;
    private final int maxDefinitionLevel;
    private final int maxRepetitionLevel;
    
    public ParquetColumn(String name, ParquetDataType dataType, boolean required) {
        this(name, dataType, required, required ? 0 : 1, 0);
    }
    
    public ParquetColumn(String name, ParquetDataType dataType, boolean required,
                        int maxDefinitionLevel, int maxRepetitionLevel) {
        this.name = name;
        this.dataType = dataType;
        this.required = required;
        this.maxDefinitionLevel = maxDefinitionLevel;
        this.maxRepetitionLevel = maxRepetitionLevel;
    }
    
    /** @return column name */
    public String getName() {
        return name;
    }
    
    /** @return data type */
    public ParquetDataType getDataType() {
        return dataType;
    }
    
    /** @return true if column is required */
    public boolean isRequired() {
        return required;
    }
    
    /** @return maximum definition level */
    public int getMaxDefinitionLevel() {
        return maxDefinitionLevel;
    }
    
    /** @return maximum repetition level */
    public int getMaxRepetitionLevel() {
        return maxRepetitionLevel;
    }
    
    @Override
    public String toString() {
        return String.format("%s: %s%s", name, dataType, required ? " (required)" : "");
    }
}