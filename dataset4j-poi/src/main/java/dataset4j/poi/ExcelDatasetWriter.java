package dataset4j.poi;

import dataset4j.Dataset;
import dataset4j.annotations.*;
import dataset4j.annotations.DataColumn;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes Dataset to Excel files using annotation-driven formatting.
 * 
 * Example usage:
 * {@code
 * ExcelDatasetWriter
 *     .toFile("output.xlsx")
 *     .sheet("Employee Report")
 *     .write(employees);
 * }
 */
public class ExcelDatasetWriter {
    
    private final String filePath;
    private String sheetName = "Sheet1";
    private boolean includeHeaders = true;
    private boolean autoSizeColumns = true;

    // Field selection support
    private PojoMetadata<?> metadata;
    private FieldSelector<?> fieldSelector;
    private List<FieldMeta> selectedFields;

    // Custom cell writers
    private CellWriter globalCellWriter;
    private final Map<String, CellWriter> fieldCellWriters = new HashMap<>();

    // Configurable default values for null fields
    private final Map<Class<?>, Object> typeDefaults = new HashMap<>();
    private final Map<String, Object> fieldDefaults = new HashMap<>();
    
    private ExcelDatasetWriter(String filePath) {
        this.filePath = filePath;
    }
    
    /**
     * Create writer for Excel file.
     * @param filePath path to output Excel file
     * @return new writer instance
     */
    public static ExcelDatasetWriter toFile(String filePath) {
        Path path = Paths.get(filePath).normalize();
        for (int i = 0; i < path.getNameCount(); i++) {
            if ("..".equals(path.getName(i).toString())) {
                throw new SecurityException("Path traversal detected in file path: " + filePath);
            }
        }
        return new ExcelDatasetWriter(filePath);
    }
    
    /**
     * Specify sheet name for output.
     * @param sheetName name of the sheet
     * @return this writer for chaining
     */
    public ExcelDatasetWriter sheet(String sheetName) {
        this.sheetName = sheetName;
        return this;
    }
    
    /**
     * Specify whether to include column headers.
     * @param includeHeaders true to include headers
     * @return this writer for chaining
     */
    public ExcelDatasetWriter headers(boolean includeHeaders) {
        this.includeHeaders = includeHeaders;
        return this;
    }
    
    /**
     * Specify whether to auto-size columns.
     * @param autoSizeColumns true to auto-size columns
     * @return this writer for chaining
     */
    public ExcelDatasetWriter autoSize(boolean autoSizeColumns) {
        this.autoSizeColumns = autoSizeColumns;
        return this;
    }
    
    /**
     * Select specific fields to export by field names.
     * @param fieldNames field names to include
     * @return this writer for chaining
     */
    public ExcelDatasetWriter fields(String... fieldNames) {
        if (metadata != null) {
            this.fieldSelector = FieldSelector.from(metadata).fields(fieldNames);
        }
        return this;
    }
    
    /**
     * Select specific fields to export by column names.
     * @param columnNames column names to include
     * @return this writer for chaining
     */
    public ExcelDatasetWriter columns(String... columnNames) {
        if (metadata != null) {
            this.fieldSelector = FieldSelector.from(metadata).columns(columnNames);
        }
        return this;
    }
    
    /**
     * Select fields using generated field constants array.
     * Designed to work with @GenerateFields generated arrays like Employee.Fields.ALL_FIELDS.
     * @param fieldConstants array of field name constants
     * @return this writer for chaining
     */
    public ExcelDatasetWriter fieldsArray(String[] fieldConstants) {
        if (metadata != null) {
            this.fieldSelector = FieldSelector.from(metadata).fieldsArray(fieldConstants);
        }
        return this;
    }
    
    /**
     * Select fields using generated column constants array.
     * Designed to work with @GenerateFields generated arrays like Employee.Fields.ALL_COLUMNS.
     * @param columnConstants array of column name constants
     * @return this writer for chaining
     */
    public ExcelDatasetWriter columnsArray(String[] columnConstants) {
        if (metadata != null) {
            this.fieldSelector = FieldSelector.from(metadata).columnsArray(columnConstants);
        }
        return this;
    }
    
    /**
     * Exclude specific fields from export.
     * @param fieldNames field names to exclude
     * @return this writer for chaining
     */
    public ExcelDatasetWriter exclude(String... fieldNames) {
        if (metadata != null) {
            if (this.fieldSelector == null) {
                this.fieldSelector = FieldSelector.from(metadata);
            }
            this.fieldSelector = this.fieldSelector.exclude(fieldNames);
        }
        return this;
    }
    
    /**
     * Select only required fields for export.
     * @return this writer for chaining
     */
    public ExcelDatasetWriter requiredOnly() {
        if (metadata != null) {
            this.fieldSelector = FieldSelector.from(metadata).requiredOnly();
        }
        return this;
    }
    
    /**
     * Select only exportable fields (not ignored or hidden).
     * @return this writer for chaining
     */
    public ExcelDatasetWriter exportableOnly() {
        if (metadata != null) {
            this.fieldSelector = FieldSelector.from(metadata).exportableOnly();
        }
        return this;
    }
    
    /**
     * Set a global custom cell writer applied to all cells.
     * @param writer the cell writer
     * @return this writer for chaining
     */
    public ExcelDatasetWriter cellWriter(CellWriter writer) {
        this.globalCellWriter = writer;
        return this;
    }

    /**
     * Set a custom cell writer for a specific field.
     * Per-field writers take priority over the global writer.
     * @param fieldName the field name to target
     * @param writer the cell writer
     * @return this writer for chaining
     */
    public ExcelDatasetWriter cellWriter(String fieldName, CellWriter writer) {
        this.fieldCellWriters.put(fieldName, writer);
        return this;
    }

    /**
     * Set the same custom cell writer for multiple fields.
     * @param writer the cell writer
     * @param fieldNames the field names to target
     * @return this writer for chaining
     */
    public ExcelDatasetWriter cellWriter(CellWriter writer, String... fieldNames) {
        for (String fieldName : fieldNames) {
            this.fieldCellWriters.put(fieldName, writer);
        }
        return this;
    }

    /**
     * Set a default value for all null fields of the given type during writing.
     * @param type the field type to configure
     * @param value the default value to write
     * @return this writer for chaining
     */
    public ExcelDatasetWriter defaultValue(Class<?> type, Object value) {
        this.typeDefaults.put(type, value);
        return this;
    }

    /**
     * Set a default value for a specific null field during writing.
     * Per-field defaults take priority over type-based defaults.
     * @param fieldName the record field name
     * @param value the default value to write
     * @return this writer for chaining
     */
    public ExcelDatasetWriter defaultValue(String fieldName, Object value) {
        this.fieldDefaults.put(fieldName, value);
        return this;
    }

    /**
     * Use pre-built metadata for field selection.
     * @param <T> record type
     * @param metadata POJO metadata
     * @return this writer for chaining
     */
    @SuppressWarnings("unchecked")
    public <T> ExcelDatasetWriter select(PojoMetadata<T> metadata) {
        this.metadata = (PojoMetadata<?>) metadata;
        return this;
    }
    
    /**
     * Use custom field selector for advanced field selection.
     * @param <T> record type
     * @param selector field selector
     * @return this writer for chaining
     */
    @SuppressWarnings("unchecked")
    public <T> ExcelDatasetWriter select(FieldSelector<T> selector) {
        this.fieldSelector = (FieldSelector<?>) selector;
        return this;
    }
    
    /**
     * Write Dataset to Excel file.
     * @param <T> record type
     * @param dataset dataset to write
     * @throws IOException if file cannot be written
     */
    public <T> void write(Dataset<T> dataset) throws IOException {
        if (!dataset.isEmpty()) {
            Class<?> recordClass = dataset.toList().get(0).getClass();
            if (!recordClass.isRecord()) {
                throw new IllegalArgumentException("Dataset must contain record types");
            }
            if (metadata == null) {
                @SuppressWarnings("unchecked")
                Class<Object> typedClass = (Class<Object>) recordClass;
                metadata = MetadataCache.getMetadata(typedClass);
            }
        }

        List<FieldMeta> fieldsToExport = resolveFields();

        try (Workbook workbook = new XSSFWorkbook();
             FileOutputStream fos = new FileOutputStream(filePath)) {

            if (dataset.isEmpty()) {
                workbook.createSheet(sheetName);
            } else {
                if (fieldsToExport.isEmpty()) {
                    throw new IllegalArgumentException("No fields selected for export");
                }
                SheetRenderConfig config = new SheetRenderConfig(
                        fieldsToExport, includeHeaders, autoSizeColumns,
                        globalCellWriter, fieldCellWriters, typeDefaults, fieldDefaults);
                ExcelSheetRenderer.renderSheet(workbook, sheetName, dataset, config);
            }

            workbook.write(fos);
        }
    }

    private List<FieldMeta> resolveFields() {
        if (fieldSelector != null) return fieldSelector.select();
        if (selectedFields != null) return selectedFields;
        if (metadata != null) return metadata.getExportableFields();
        return java.util.Collections.emptyList();
    }
}
