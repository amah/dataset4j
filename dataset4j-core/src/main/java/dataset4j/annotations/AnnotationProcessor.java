package dataset4j.annotations;

import java.lang.reflect.RecordComponent;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Utility class for extracting column metadata from annotated record classes.
 * 
 * <p>This processor scans record components for @DataColumn
 * annotations and builds unified metadata for export/import operations.
 * 
 * Example usage:
 * {@code
 * public record Employee(
 *     @DataColumn(name = "ID", order = 1)
 *     String id,
 *     
 *     @DataColumn(name = "Name", order = 2)
 *     String name
 * ) {}
 * 
 * // Usage
 * List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(Employee.class);
 * ColumnMetadata idColumn = AnnotationProcessor.findColumn(Employee.class, "id");
 * }
 */
public final class AnnotationProcessor {
    
    private AnnotationProcessor() {
        // Utility class
    }
    
    /**
     * Extract all column metadata from a record class.
     * 
     * @param recordClass the record class to process
     * @return list of column metadata, ordered by specified order or field declaration order
     */
    public static List<ColumnMetadata> extractColumns(Class<?> recordClass) {
        if (!recordClass.isRecord()) {
            throw new IllegalArgumentException("Class must be a record: " + recordClass.getName());
        }
        
        RecordComponent[] components = recordClass.getRecordComponents();
        List<ColumnMetadata> columns = new ArrayList<>();
        
        for (int i = 0; i < components.length; i++) {
            RecordComponent component = components[i];
            
            // Only process components with @DataColumn annotation
            if (component.isAnnotationPresent(DataColumn.class)) {
                ColumnMetadata metadata = extractColumnMetadata(component, i);
                
                if (!metadata.isIgnored()) {
                    columns.add(metadata);
                }
            }
        }
        
        // Sort by order (explicit order first, then declaration order)
        columns.sort((a, b) -> {
            int orderA = a.getOrder() != -1 ? a.getOrder() : 1000 + getDeclarationIndex(recordClass, a.getFieldName());
            int orderB = b.getOrder() != -1 ? b.getOrder() : 1000 + getDeclarationIndex(recordClass, b.getFieldName());
            return Integer.compare(orderA, orderB);
        });
        
        return columns;
    }
    
    /**
     * Extract metadata for a specific field by name.
     * 
     * @param recordClass the record class
     * @param fieldName the field name to find
     * @return column metadata, or null if field not found
     */
    public static ColumnMetadata findColumn(Class<?> recordClass, String fieldName) {
        if (!recordClass.isRecord()) {
            throw new IllegalArgumentException("Class must be a record: " + recordClass.getName());
        }
        
        RecordComponent[] components = recordClass.getRecordComponents();
        for (int i = 0; i < components.length; i++) {
            RecordComponent component = components[i];
            if (component.getName().equals(fieldName)) {
                return extractColumnMetadata(component, i);
            }
        }
        return null;
    }
    
    /**
     * Get all column names in order for use as headers.
     * 
     * @param recordClass the record class
     * @return ordered list of column names
     */
    public static List<String> getColumnHeaders(Class<?> recordClass) {
        return extractColumns(recordClass).stream()
                .map(ColumnMetadata::getEffectiveColumnName)
                .collect(Collectors.toList());
    }
    
    /**
     * Get mapping from field names to column names.
     * 
     * @param recordClass the record class
     * @return map of field name to column name
     */
    public static Map<String, String> getFieldToColumnMapping(Class<?> recordClass) {
        return extractColumns(recordClass).stream()
                .collect(Collectors.toMap(
                    ColumnMetadata::getFieldName,
                    ColumnMetadata::getEffectiveColumnName,
                    (existing, replacement) -> existing,
                    LinkedHashMap::new
                ));
    }
    
    /**
     * Check if a record class has any column annotations.
     * 
     * @param recordClass the record class to check
     * @return true if any field has column annotations
     */
    public static boolean hasColumnAnnotations(Class<?> recordClass) {
        if (!recordClass.isRecord()) {
            return false;
        }
        
        RecordComponent[] components = recordClass.getRecordComponents();
        for (RecordComponent component : components) {
            if (component.isAnnotationPresent(DataColumn.class)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Extract column metadata from a single record component.
     */
    private static ColumnMetadata extractColumnMetadata(RecordComponent component, int declarationIndex) {
        ColumnMetadata.Builder builder = ColumnMetadata.builder(component);
        
        // Process @DataColumn annotation
        DataColumn dataColumn = component.getAnnotation(DataColumn.class);
        if (dataColumn != null) {
            builder.columnName(dataColumn.name())
                   .order(dataColumn.order())
                   .required(dataColumn.required())
                   .ignored(dataColumn.ignore())
                   .description(dataColumn.description())
                   .defaultValue(dataColumn.defaultValue());
            
            // Use columnIndex if specified, otherwise use order
            if (dataColumn.columnIndex() != -1) {
                builder.order(dataColumn.columnIndex() + 1); // Convert 0-based to 1-based for ordering
            }
            
            if (!dataColumn.numberFormat().isEmpty()) {
                builder.numberFormat(dataColumn.numberFormat());
            }
            if (!dataColumn.dateFormat().isEmpty()) {
                builder.dateFormat(dataColumn.dateFormat());
            }
            if (dataColumn.width() != -1) {
                builder.maxLength(dataColumn.width());
            }
        }
        
        return builder.build();
    }
    
    /**
     * Get the declaration index of a field in the record.
     */
    private static int getDeclarationIndex(Class<?> recordClass, String fieldName) {
        RecordComponent[] components = recordClass.getRecordComponents();
        for (int i = 0; i < components.length; i++) {
            if (components[i].getName().equals(fieldName)) {
                return i;
            }
        }
        return Integer.MAX_VALUE; // Field not found
    }
}