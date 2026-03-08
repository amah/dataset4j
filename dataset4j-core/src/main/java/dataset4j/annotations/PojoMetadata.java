package dataset4j.annotations;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Complete metadata for a record class, including all field metadata and table-level information.
 * This class serves as the foundation for field selection and Excel export functionality.
 * 
 * <p>Example usage:
 * {@code
 * PojoMetadata<Employee> metadata = PojoMetadata.of(Employee.class);
 * 
 * // Get all field names
 * List<String> fields = metadata.getFieldNames();
 * 
 * // Get specific field metadata
 * FieldMeta idField = metadata.getField("id");
 * 
 * // Get ordered fields for export
 * List<FieldMeta> exportFields = metadata.getExportableFields();
 * }
 */
public final class PojoMetadata<T> {
    
    private final Class<T> recordClass;
    private final String tableName;
    private final Map<String, FieldMeta> fieldMap;
    private final List<FieldMeta> orderedFields;
    
    private PojoMetadata(Class<T> recordClass, String tableName, List<FieldMeta> fields) {
        this.recordClass = recordClass;
        this.tableName = tableName;
        this.fieldMap = fields.stream()
                .collect(Collectors.toMap(
                    FieldMeta::getFieldName,
                    field -> field,
                    (existing, replacement) -> existing,
                    LinkedHashMap::new
                ));
        this.orderedFields = Collections.unmodifiableList(new ArrayList<>(fields));
    }
    
    /**
     * Get the record class this metadata describes.
     */
    public Class<T> getRecordClass() {
        return recordClass;
    }
    
    /**
     * Get the table name (class simple name by default).
     */
    public String getTableName() {
        return tableName;
    }
    
    /**
     * Get all field names in declaration order.
     */
    public List<String> getFieldNames() {
        return orderedFields.stream()
                .map(FieldMeta::getFieldName)
                .collect(Collectors.toList());
    }
    
    /**
     * Get metadata for a specific field by name.
     * @param fieldName the field name
     * @return field metadata, or null if not found
     */
    public FieldMeta getField(String fieldName) {
        return fieldMap.get(fieldName);
    }
    
    /**
     * Get all field metadata in order.
     */
    public List<FieldMeta> getAllFields() {
        return orderedFields;
    }
    
    /**
     * Get only the fields that should be exported (not ignored or hidden).
     */
    public List<FieldMeta> getExportableFields() {
        return orderedFields.stream()
                .filter(field -> !field.isIgnored() && !field.isHidden())
                .collect(Collectors.toList());
    }
    
    /**
     * Get fields by their column names.
     */
    public List<FieldMeta> getFieldsByColumnNames(List<String> columnNames) {
        Map<String, FieldMeta> columnMap = orderedFields.stream()
                .collect(Collectors.toMap(
                    FieldMeta::getEffectiveColumnName,
                    field -> field,
                    (existing, replacement) -> existing
                ));
        
        return columnNames.stream()
                .map(columnMap::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
    
    /**
     * Get fields by their field names (preserving order).
     */
    public List<FieldMeta> getFieldsByNames(List<String> fieldNames) {
        return fieldNames.stream()
                .map(fieldMap::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
    
    /**
     * Get column headers for export.
     */
    public List<String> getColumnHeaders() {
        return getExportableFields().stream()
                .map(FieldMeta::getEffectiveColumnName)
                .collect(Collectors.toList());
    }
    
    /**
     * Get mapping from field names to column names.
     */
    public Map<String, String> getFieldToColumnMapping() {
        return orderedFields.stream()
                .collect(Collectors.toMap(
                    FieldMeta::getFieldName,
                    FieldMeta::getEffectiveColumnName,
                    (existing, replacement) -> existing,
                    LinkedHashMap::new
                ));
    }
    
    /**
     * Check if a field exists by name.
     */
    public boolean hasField(String fieldName) {
        return fieldMap.containsKey(fieldName);
    }
    
    /**
     * Check if any field has formatting rules.
     */
    public boolean hasFormattedFields() {
        return orderedFields.stream().anyMatch(FieldMeta::hasFormatting);
    }
    
    /**
     * Get required field names.
     */
    public List<String> getRequiredFields() {
        return orderedFields.stream()
                .filter(FieldMeta::isRequired)
                .map(FieldMeta::getFieldName)
                .collect(Collectors.toList());
    }
    
    /**
     * Get fields with specific cell type.
     */
    public List<FieldMeta> getFieldsByCellType(DataColumn.CellType cellType) {
        return orderedFields.stream()
                .filter(field -> field.getCellType() == cellType)
                .collect(Collectors.toList());
    }
    
    /**
     * Convert to legacy ColumnMetadata list for backward compatibility.
     * This is disabled for now due to RecordComponent limitations.
     */
    public List<ColumnMetadata> toColumnMetadata() {
        // TODO: Implement proper conversion when needed
        return new ArrayList<>();
    }
    
    @Override
    public String toString() {
        return String.format("PojoMetadata[class=%s, fields=%d, table=%s]",
                recordClass.getSimpleName(), orderedFields.size(), tableName);
    }
    
    /**
     * Create metadata from a record class using runtime reflection.
     * This is the runtime alternative to compile-time metadata generation.
     */
    public static <T> PojoMetadata<T> of(Class<T> recordClass) {
        if (!recordClass.isRecord()) {
            throw new IllegalArgumentException("Class must be a record: " + recordClass.getName());
        }
        
        // Extract table name from @DataTable annotation or use class name
        String tableName = recordClass.getSimpleName();
        DataTable tableAnnotation = recordClass.getAnnotation(DataTable.class);
        if (tableAnnotation != null && !tableAnnotation.name().isEmpty()) {
            tableName = tableAnnotation.name();
        }
        
        // Extract field metadata using AnnotationProcessor
        List<ColumnMetadata> columnMetadata = AnnotationProcessor.extractColumns(recordClass);
        List<FieldMeta> fields = columnMetadata.stream()
                .map(PojoMetadata::convertToFieldMeta)
                .collect(Collectors.toList());
        
        return new PojoMetadata<>(recordClass, tableName, fields);
    }
    
    /**
     * Create metadata with specific fields (for field selection).
     */
    public static <T> PojoMetadata<T> withFields(Class<T> recordClass, List<FieldMeta> fields) {
        String tableName = recordClass.getSimpleName();
        DataTable tableAnnotation = recordClass.getAnnotation(DataTable.class);
        if (tableAnnotation != null && !tableAnnotation.name().isEmpty()) {
            tableName = tableAnnotation.name();
        }
        
        return new PojoMetadata<>(recordClass, tableName, fields);
    }
    
    /**
     * Convert ColumnMetadata to FieldMeta for integration.
     */
    private static FieldMeta convertToFieldMeta(ColumnMetadata columnMeta) {
        return FieldMeta.builder()
                .fieldName(columnMeta.getFieldName())
                .columnName(columnMeta.getColumnName())
                .fieldType(columnMeta.getFieldType())
                .order(columnMeta.getOrder())
                .required(columnMeta.isRequired())
                .ignored(columnMeta.isIgnored())
                .description(columnMeta.getDescription())
                .defaultValue(columnMeta.getDefaultValue())
                .numberFormat(columnMeta.getNumberFormat())
                .dateFormat(columnMeta.getDateFormat())
                .width(columnMeta.getMaxLength())
                .build();
    }
}