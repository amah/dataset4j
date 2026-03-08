package dataset4j.annotations;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Utility for selecting and filtering fields from POJO metadata.
 * Provides fluent API for building field selection criteria.
 * 
 * <p>Example usage:
 * {@code
 * // Select specific fields
 * List<FieldMeta> fields = FieldSelector.from(metadata)
 *     .fields("id", "name", "email")
 *     .select();
 * 
 * // Select all except specific fields
 * List<FieldMeta> fields = FieldSelector.from(metadata)
 *     .exclude("password", "internalNotes")
 *     .select();
 * 
 * // Select fields with conditions
 * List<FieldMeta> requiredFields = FieldSelector.from(metadata)
 *     .where(field -> field.isRequired())
 *     .select();
 * 
 * // Complex selection
 * List<FieldMeta> exportFields = FieldSelector.from(metadata)
 *     .fields("id", "name", "email", "salary")
 *     .exclude("internalNotes")
 *     .where(field -> !field.isHidden())
 *     .orderBy(FieldMeta::getOrder)
 *     .select();
 * }
 */
public final class FieldSelector<T> {
    
    private final PojoMetadata<T> metadata;
    private Set<String> includedFields;
    private Set<String> excludedFields;
    private List<Predicate<FieldMeta>> conditions;
    private Comparator<FieldMeta> ordering;
    private boolean useFieldNames = true; // true for field names, false for column names
    
    private FieldSelector(PojoMetadata<T> metadata) {
        this.metadata = metadata;
        this.includedFields = new LinkedHashSet<>();
        this.excludedFields = new LinkedHashSet<>();
        this.conditions = new ArrayList<>();
        this.ordering = Comparator.comparing(FieldMeta::getOrder)
                .thenComparing(field -> metadata.getAllFields().indexOf(field));
    }
    
    /**
     * Create a selector from POJO metadata.
     */
    public static <T> FieldSelector<T> from(PojoMetadata<T> metadata) {
        return new FieldSelector<>(metadata);
    }
    
    /**
     * Create a selector from a record class.
     */
    public static <T> FieldSelector<T> from(Class<T> recordClass) {
        return new FieldSelector<>(PojoMetadata.of(recordClass));
    }
    
    /**
     * Select specific fields by name.
     * @param fieldNames field names to include
     * @return this selector for chaining
     */
    public FieldSelector<T> fields(String... fieldNames) {
        this.includedFields.addAll(Arrays.asList(fieldNames));
        this.useFieldNames = true;
        return this;
    }
    
    /**
     * Select fields by field names from collection.
     * @param fieldNames collection of field names
     * @return this selector for chaining
     */
    public FieldSelector<T> fields(Collection<String> fieldNames) {
        this.includedFields.addAll(fieldNames);
        this.useFieldNames = true;
        return this;
    }
    
    /**
     * Select fields using generated field constants array.
     * Designed to work with @GenerateFields generated arrays like Employee.Fields.ALL_FIELDS.
     * @param fieldConstants array of field name constants
     * @return this selector for chaining
     */
    public FieldSelector<T> fieldsArray(String[] fieldConstants) {
        return fields(Arrays.asList(fieldConstants));
    }
    
    /**
     * Select specific fields by column name.
     * @param columnNames column names to include
     * @return this selector for chaining
     */
    public FieldSelector<T> columns(String... columnNames) {
        this.includedFields.addAll(Arrays.asList(columnNames));
        this.useFieldNames = false;
        return this;
    }
    
    /**
     * Select fields by column names from collection.
     * @param columnNames collection of column names
     * @return this selector for chaining
     */
    public FieldSelector<T> columns(Collection<String> columnNames) {
        this.includedFields.addAll(columnNames);
        this.useFieldNames = false;
        return this;
    }
    
    /**
     * Select fields using generated column constants array.
     * Designed to work with @GenerateFields generated arrays like Employee.Fields.ALL_COLUMNS.
     * @param columnConstants array of column name constants
     * @return this selector for chaining
     */
    public FieldSelector<T> columnsArray(String[] columnConstants) {
        return columns(Arrays.asList(columnConstants));
    }
    
    /**
     * Exclude specific fields from selection.
     * @param fieldNames field names to exclude
     * @return this selector for chaining
     */
    public FieldSelector<T> exclude(String... fieldNames) {
        this.excludedFields.addAll(Arrays.asList(fieldNames));
        return this;
    }
    
    /**
     * Exclude fields from collection.
     * @param fieldNames collection of field names to exclude
     * @return this selector for chaining
     */
    public FieldSelector<T> exclude(Collection<String> fieldNames) {
        this.excludedFields.addAll(fieldNames);
        return this;
    }
    
    /**
     * Exclude fields using generated field constants array.
     * @param fieldConstants array of field name constants to exclude
     * @return this selector for chaining
     */
    public FieldSelector<T> excludeArray(String[] fieldConstants) {
        return exclude(Arrays.asList(fieldConstants));
    }
    
    /**
     * Add a condition filter for field selection.
     * @param condition predicate to test fields
     * @return this selector for chaining
     */
    public FieldSelector<T> where(Predicate<FieldMeta> condition) {
        this.conditions.add(condition);
        return this;
    }
    
    /**
     * Select only required fields.
     * @return this selector for chaining
     */
    public FieldSelector<T> requiredOnly() {
        return where(FieldMeta::isRequired);
    }
    
    /**
     * Select only exportable fields (not ignored or hidden).
     * @return this selector for chaining
     */
    public FieldSelector<T> exportableOnly() {
        return where(field -> !field.isIgnored() && !field.isHidden());
    }
    
    /**
     * Select fields of specific type.
     * @param fieldType the field type to match
     * @return this selector for chaining
     */
    public FieldSelector<T> ofType(Class<?> fieldType) {
        return where(field -> field.getFieldType() == fieldType);
    }
    
    /**
     * Select fields with specific cell type.
     * @param cellType the cell type to match
     * @return this selector for chaining
     */
    public FieldSelector<T> withCellType(DataColumn.CellType cellType) {
        return where(field -> field.getCellType() == cellType);
    }
    
    /**
     * Select fields with formatting rules.
     * @return this selector for chaining
     */
    public FieldSelector<T> withFormatting() {
        return where(FieldMeta::hasFormatting);
    }
    
    /**
     * Set custom ordering for selected fields.
     * @param comparator field comparator
     * @return this selector for chaining
     */
    public FieldSelector<T> orderBy(Comparator<FieldMeta> comparator) {
        this.ordering = comparator;
        return this;
    }
    
    /**
     * Order by field order annotation.
     * @return this selector for chaining
     */
    public FieldSelector<T> orderByAnnotation() {
        this.ordering = Comparator.comparing(FieldMeta::getOrder)
                .thenComparing(field -> metadata.getAllFields().indexOf(field));
        return this;
    }
    
    /**
     * Order by field declaration order.
     * @return this selector for chaining
     */
    public FieldSelector<T> orderByDeclaration() {
        this.ordering = Comparator.comparing(field -> metadata.getAllFields().indexOf(field));
        return this;
    }
    
    /**
     * Order by field name alphabetically.
     * @return this selector for chaining
     */
    public FieldSelector<T> orderByName() {
        this.ordering = Comparator.comparing(FieldMeta::getFieldName);
        return this;
    }
    
    /**
     * Execute the selection and return matching fields.
     * @return list of selected field metadata
     */
    public List<FieldMeta> select() {
        List<FieldMeta> candidates;
        
        if (includedFields.isEmpty()) {
            // No specific inclusion - start with all fields
            candidates = new ArrayList<>(metadata.getAllFields());
        } else {
            // Get specifically included fields
            if (useFieldNames) {
                candidates = metadata.getFieldsByNames(new ArrayList<>(includedFields));
            } else {
                candidates = metadata.getFieldsByColumnNames(new ArrayList<>(includedFields));
            }
        }
        
        // Apply exclusions
        if (!excludedFields.isEmpty()) {
            candidates = candidates.stream()
                    .filter(field -> !excludedFields.contains(field.getFieldName()) &&
                                   !excludedFields.contains(field.getEffectiveColumnName()))
                    .collect(Collectors.toList());
        }
        
        // Apply conditions
        for (Predicate<FieldMeta> condition : conditions) {
            candidates = candidates.stream()
                    .filter(condition)
                    .collect(Collectors.toList());
        }
        
        // Apply ordering
        if (ordering != null) {
            candidates.sort(ordering);
        }
        
        return candidates;
    }
    
    /**
     * Execute selection and return field names.
     * @return list of selected field names
     */
    public List<String> selectNames() {
        return select().stream()
                .map(FieldMeta::getFieldName)
                .collect(Collectors.toList());
    }
    
    /**
     * Execute selection and return column names.
     * @return list of selected column names
     */
    public List<String> selectColumnNames() {
        return select().stream()
                .map(FieldMeta::getEffectiveColumnName)
                .collect(Collectors.toList());
    }
    
    /**
     * Execute selection and create new metadata with selected fields.
     * @return new PojoMetadata with selected fields only
     */
    public PojoMetadata<T> selectAsMetadata() {
        List<FieldMeta> selectedFields = select();
        return PojoMetadata.withFields(metadata.getRecordClass(), selectedFields);
    }
    
    /**
     * Count matching fields without executing full selection.
     * @return number of fields that would be selected
     */
    public int count() {
        return select().size();
    }
    
    /**
     * Check if any fields match the selection criteria.
     * @return true if at least one field matches
     */
    public boolean hasMatches() {
        return count() > 0;
    }
}