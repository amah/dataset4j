package dataset4j.annotations;

import java.lang.reflect.RecordComponent;
import java.util.Objects;

/**
 * Immutable metadata for a single field/column in a record class.
 * This is the core building block for compile-time metadata generation.
 * 
 * <p>Example usage:
 * {@code
 * FieldMeta idMeta = FieldMeta.builder()
 *     .fieldName("id")
 *     .columnName("Employee ID")
 *     .fieldType(String.class)
 *     .order(1)
 *     .required(true)
 *     .build();
 * }
 */
public final class FieldMeta {
    
    private final String fieldName;
    private final String columnName;
    private final Class<?> fieldType;
    private final int order;
    private final boolean required;
    private final boolean ignored;
    private final String description;
    private final String defaultValue;
    private final boolean hidden;
    
    // Formatting properties
    private final DataColumn.CellType cellType;
    private final String numberFormat;
    private final String dateFormat;
    private final String backgroundColor;
    private final String fontColor;
    private final boolean bold;
    private final boolean frozen;
    private final int width;
    private final boolean wrapText;
    private final DataColumn.Alignment alignment;
    
    private FieldMeta(Builder builder) {
        this.fieldName = builder.fieldName;
        this.columnName = builder.columnName;
        this.fieldType = builder.fieldType;
        this.order = builder.order;
        this.required = builder.required;
        this.ignored = builder.ignored;
        this.description = builder.description;
        this.defaultValue = builder.defaultValue;
        this.hidden = builder.hidden;
        this.cellType = builder.cellType;
        this.numberFormat = builder.numberFormat;
        this.dateFormat = builder.dateFormat;
        this.backgroundColor = builder.backgroundColor;
        this.fontColor = builder.fontColor;
        this.bold = builder.bold;
        this.frozen = builder.frozen;
        this.width = builder.width;
        this.wrapText = builder.wrapText;
        this.alignment = builder.alignment;
    }
    
    // Getters
    public String getFieldName() { return fieldName; }
    public String getColumnName() { return columnName; }
    public Class<?> getFieldType() { return fieldType; }
    public int getOrder() { return order; }
    public boolean isRequired() { return required; }
    public boolean isIgnored() { return ignored; }
    public String getDescription() { return description; }
    public String getDefaultValue() { return defaultValue; }
    public boolean isHidden() { return hidden; }
    public DataColumn.CellType getCellType() { return cellType; }
    public String getNumberFormat() { return numberFormat; }
    public String getDateFormat() { return dateFormat; }
    public String getBackgroundColor() { return backgroundColor; }
    public String getFontColor() { return fontColor; }
    public boolean isBold() { return bold; }
    public boolean isFrozen() { return frozen; }
    public int getWidth() { return width; }
    public boolean isWrapText() { return wrapText; }
    public DataColumn.Alignment getAlignment() { return alignment; }
    
    /**
     * Get the effective column name, falling back to field name if not specified.
     */
    public String getEffectiveColumnName() {
        return columnName.isEmpty() ? fieldName : columnName;
    }
    
    /**
     * Check if this field has custom formatting.
     */
    public boolean hasFormatting() {
        return cellType != DataColumn.CellType.AUTO ||
               !numberFormat.isEmpty() ||
               !dateFormat.equals("yyyy-MM-dd") ||
               !backgroundColor.isEmpty() ||
               !fontColor.isEmpty() ||
               bold ||
               frozen ||
               width != -1 ||
               wrapText ||
               alignment != DataColumn.Alignment.AUTO;
    }
    
    /**
     * Convert to ColumnMetadata for backward compatibility.
     * Since we can't create synthetic RecordComponents, we'll create a simple implementation.
     */
    public ColumnMetadata toColumnMetadata() {
        // For now, we'll skip the RecordComponent and create ColumnMetadata directly
        // This is a temporary solution - in a real implementation, we'd need to handle this properly
        return null; // Placeholder - will be implemented when needed
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        FieldMeta that = (FieldMeta) obj;
        return Objects.equals(fieldName, that.fieldName);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(fieldName);
    }
    
    @Override
    public String toString() {
        return String.format("FieldMeta[field=%s, column=%s, type=%s, order=%d]",
                fieldName, getEffectiveColumnName(), fieldType.getSimpleName(), order);
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String fieldName = "";
        private String columnName = "";
        private Class<?> fieldType = Object.class;
        private int order = -1;
        private boolean required = false;
        private boolean ignored = false;
        private String description = "";
        private String defaultValue = "";
        private boolean hidden = false;
        private DataColumn.CellType cellType = DataColumn.CellType.AUTO;
        private String numberFormat = "";
        private String dateFormat = "yyyy-MM-dd";
        private String backgroundColor = "";
        private String fontColor = "";
        private boolean bold = false;
        private boolean frozen = false;
        private int width = -1;
        private boolean wrapText = false;
        private DataColumn.Alignment alignment = DataColumn.Alignment.AUTO;
        
        public Builder fieldName(String fieldName) {
            this.fieldName = fieldName != null ? fieldName : "";
            return this;
        }
        
        public Builder columnName(String columnName) {
            this.columnName = columnName != null ? columnName : "";
            return this;
        }
        
        public Builder fieldType(Class<?> fieldType) {
            this.fieldType = fieldType != null ? fieldType : Object.class;
            return this;
        }
        
        public Builder order(int order) {
            this.order = order;
            return this;
        }
        
        public Builder required(boolean required) {
            this.required = required;
            return this;
        }
        
        public Builder ignored(boolean ignored) {
            this.ignored = ignored;
            return this;
        }
        
        public Builder description(String description) {
            this.description = description != null ? description : "";
            return this;
        }
        
        public Builder defaultValue(String defaultValue) {
            this.defaultValue = defaultValue != null ? defaultValue : "";
            return this;
        }
        
        public Builder hidden(boolean hidden) {
            this.hidden = hidden;
            return this;
        }
        
        public Builder cellType(DataColumn.CellType cellType) {
            this.cellType = cellType != null ? cellType : DataColumn.CellType.AUTO;
            return this;
        }
        
        public Builder numberFormat(String numberFormat) {
            this.numberFormat = numberFormat != null ? numberFormat : "";
            return this;
        }
        
        public Builder dateFormat(String dateFormat) {
            this.dateFormat = dateFormat != null ? dateFormat : "yyyy-MM-dd";
            return this;
        }
        
        public Builder backgroundColor(String backgroundColor) {
            this.backgroundColor = backgroundColor != null ? backgroundColor : "";
            return this;
        }
        
        public Builder fontColor(String fontColor) {
            this.fontColor = fontColor != null ? fontColor : "";
            return this;
        }
        
        public Builder bold(boolean bold) {
            this.bold = bold;
            return this;
        }
        
        public Builder frozen(boolean frozen) {
            this.frozen = frozen;
            return this;
        }
        
        public Builder width(int width) {
            this.width = width;
            return this;
        }
        
        public Builder wrapText(boolean wrapText) {
            this.wrapText = wrapText;
            return this;
        }
        
        public Builder alignment(DataColumn.Alignment alignment) {
            this.alignment = alignment != null ? alignment : DataColumn.Alignment.AUTO;
            return this;
        }
        
        public FieldMeta build() {
            return new FieldMeta(this);
        }
    }
    
    /**
     * Create a synthetic RecordComponent for backward compatibility.
     * Since RecordComponent is final, we use a wrapper approach.
     */
    private static RecordComponent createSyntheticRecordComponent(String name, Class<?> type) {
        // We need to find an actual record component from any record to use as a template
        // For now, we'll return null and handle this in the ColumnMetadata conversion
        return null;
    }
}