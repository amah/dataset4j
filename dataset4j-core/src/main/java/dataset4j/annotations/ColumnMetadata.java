package dataset4j.annotations;

import java.lang.reflect.RecordComponent;
import java.util.Objects;

/**
 * Metadata container for a single column extracted from record annotations.
 * 
 * <p>This class provides a unified view of column information regardless of the
 * specific annotation type (@Column, @ExcelColumn, @CsvColumn).
 */
public final class ColumnMetadata {
    
    private final RecordComponent recordComponent;
    private final String fieldName;
    private final String columnName;
    private final int order;
    private final boolean required;
    private final boolean ignored;
    private final String description;
    private final String defaultValue;
    private final Class<?> fieldType;
    
    // Format-specific metadata
    private final String numberFormat;
    private final String dateFormat;
    private final String[] alternativeDateFormats;
    private final int maxLength;
    private final DataColumn.WriteAs writeAs;

    private ColumnMetadata(Builder builder) {
        this.recordComponent = builder.recordComponent;
        this.fieldName = builder.fieldName;
        this.columnName = builder.columnName;
        this.order = builder.order;
        this.required = builder.required;
        this.ignored = builder.ignored;
        this.description = builder.description;
        this.defaultValue = builder.defaultValue;
        this.fieldType = builder.fieldType;
        this.numberFormat = builder.numberFormat;
        this.dateFormat = builder.dateFormat;
        this.alternativeDateFormats = builder.alternativeDateFormats;
        this.maxLength = builder.maxLength;
        this.writeAs = builder.writeAs;
    }
    
    /** @return the record component this metadata describes */
    public RecordComponent getRecordComponent() { return recordComponent; }
    /** @return the field name */
    public String getFieldName() { return fieldName; }
    /** @return the column name */
    public String getColumnName() { return columnName; }
    /** @return the column order */
    public int getOrder() { return order; }
    /** @return true if field is required */
    public boolean isRequired() { return required; }
    /** @return true if field is ignored */
    public boolean isIgnored() { return ignored; }
    /** @return the field description */
    public String getDescription() { return description; }
    /** @return the default value */
    public String getDefaultValue() { return defaultValue; }
    /** @return the field type */
    public Class<?> getFieldType() { return fieldType; }
    /** @return the number format pattern */
    public String getNumberFormat() { return numberFormat; }
    /** @return the date format pattern */
    public String getDateFormat() { return dateFormat; }
    /** @return the alternative date format patterns */
    public String[] getAlternativeDateFormats() { return alternativeDateFormats; }
    /** @return the maximum field length */
    public int getMaxLength() { return maxLength; }
    /** @return the write-as mode */
    public DataColumn.WriteAs getWriteAs() { return writeAs; }
    
    /**
     * Get the effective column name, falling back to field name if not specified.
     * @return the effective column name
     */
    public String getEffectiveColumnName() {
        return columnName.isEmpty() ? fieldName : columnName;
    }
    
    /**
     * Check if this column has formatting rules.
     * @return true if column has formatting
     */
    public boolean hasFormatting() {
        return !numberFormat.isEmpty() || !dateFormat.equals("yyyy-MM-dd");
    }
    
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ColumnMetadata that = (ColumnMetadata) obj;
        return Objects.equals(fieldName, that.fieldName);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(fieldName);
    }
    
    @Override
    public String toString() {
        return String.format("ColumnMetadata[field=%s, column=%s, order=%d, type=%s]", 
                           fieldName, getEffectiveColumnName(), order, fieldType.getSimpleName());
    }
    
    /**
     * Create a builder for ColumnMetadata.
     * @param recordComponent the record component
     * @return a new builder instance
     */
    public static Builder builder(RecordComponent recordComponent) {
        return new Builder(recordComponent);
    }
    
    /** Builder for creating ColumnMetadata instances. */
    public static class Builder {
        private final RecordComponent recordComponent;
        private final String fieldName;
        private final Class<?> fieldType;
        private String columnName = "";
        private int order = -1;
        private boolean required = false;
        private boolean ignored = false;
        private String description = "";
        private String defaultValue = "";
        private String numberFormat = "";
        private String dateFormat = "yyyy-MM-dd";
        private String[] alternativeDateFormats = {};
        private int maxLength = -1;
        private DataColumn.WriteAs writeAs = DataColumn.WriteAs.AUTO;

        /** Constructor. @param recordComponent the record component */
        public Builder(RecordComponent recordComponent) {
            this.recordComponent = recordComponent;
            this.fieldName = recordComponent.getName();
            this.fieldType = recordComponent.getType();
        }
        
        /** @param columnName column name @return this builder */
        public Builder columnName(String columnName) {
            this.columnName = columnName != null ? columnName : "";
            return this;
        }
        
        /** @param order column order @return this builder */
        public Builder order(int order) {
            this.order = order;
            return this;
        }
        
        /** @param required whether required @return this builder */
        public Builder required(boolean required) {
            this.required = required;
            return this;
        }
        
        /** @param ignored whether ignored @return this builder */
        public Builder ignored(boolean ignored) {
            this.ignored = ignored;
            return this;
        }
        
        /** @param description field description @return this builder */
        public Builder description(String description) {
            this.description = description != null ? description : "";
            return this;
        }
        
        /** @param defaultValue default value @return this builder */
        public Builder defaultValue(String defaultValue) {
            this.defaultValue = defaultValue != null ? defaultValue : "";
            return this;
        }
        
        /** @param numberFormat number format @return this builder */
        public Builder numberFormat(String numberFormat) {
            this.numberFormat = numberFormat != null ? numberFormat : "";
            return this;
        }
        
        /** @param dateFormat date format @return this builder */
        public Builder dateFormat(String dateFormat) {
            this.dateFormat = dateFormat != null ? dateFormat : "yyyy-MM-dd";
            return this;
        }
        
        /** @param alternativeDateFormats alternative date formats @return this builder */
        public Builder alternativeDateFormats(String[] alternativeDateFormats) {
            this.alternativeDateFormats = alternativeDateFormats != null ? alternativeDateFormats : new String[0];
            return this;
        }
        
        /** @param maxLength max length @return this builder */
        public Builder maxLength(int maxLength) {
            this.maxLength = maxLength;
            return this;
        }

        /** @param writeAs write-as mode @return this builder */
        public Builder writeAs(DataColumn.WriteAs writeAs) {
            this.writeAs = writeAs != null ? writeAs : DataColumn.WriteAs.AUTO;
            return this;
        }

        /** @return built ColumnMetadata instance */
        public ColumnMetadata build() {
            return new ColumnMetadata(this);
        }
    }
}