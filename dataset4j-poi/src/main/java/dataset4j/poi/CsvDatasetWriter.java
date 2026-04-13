package dataset4j.poi;

import com.opencsv.CSVWriter;
import dataset4j.CellValue;
import dataset4j.Dataset;
import dataset4j.Table;
import dataset4j.annotations.*;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.RecordComponent;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Writes Dataset to CSV files using annotation-driven mapping.
 *
 * Example usage:
 * {@code
 * CsvDatasetWriter
 *     .toFile("output.csv")
 *     .headers(true)
 *     .write(employees);
 *
 * // With field selection
 * CsvDatasetWriter
 *     .toFile("output.csv")
 *     .select(metadata)
 *     .fields("name", "email")
 *     .write(employees);
 * }
 */
public class CsvDatasetWriter {

    private final String filePath;
    private boolean includeHeaders = true;
    private char separator = CSVWriter.DEFAULT_SEPARATOR;
    private char quoteChar = CSVWriter.DEFAULT_QUOTE_CHARACTER;
    private char escapeChar = CSVWriter.DEFAULT_ESCAPE_CHARACTER;
    private String lineEnd = CSVWriter.DEFAULT_LINE_END;

    // Field selection support
    private PojoMetadata<?> metadata;
    private FieldSelector<?> fieldSelector;
    private List<FieldMeta> selectedFields;

    private CsvDatasetWriter(String filePath) {
        this.filePath = filePath;
    }

    /**
     * Create writer for CSV file.
     * @param filePath path to output CSV file
     * @return new writer instance
     */
    public static CsvDatasetWriter toFile(String filePath) {
        Path path = Paths.get(filePath).normalize();
        if (path.toString().contains("..")) {
            throw new SecurityException("Path traversal detected in file path: " + filePath);
        }
        return new CsvDatasetWriter(filePath);
    }

    /**
     * Specify whether to include column headers.
     * @param includeHeaders true to include headers
     * @return this writer for chaining
     */
    public CsvDatasetWriter headers(boolean includeHeaders) {
        this.includeHeaders = includeHeaders;
        return this;
    }

    /**
     * Set CSV separator character.
     * @param separator separator character (default: comma)
     * @return this writer for chaining
     */
    public CsvDatasetWriter separator(char separator) {
        this.separator = separator;
        return this;
    }

    /**
     * Set CSV quote character.
     * @param quoteChar quote character
     * @return this writer for chaining
     */
    public CsvDatasetWriter quoteChar(char quoteChar) {
        this.quoteChar = quoteChar;
        return this;
    }

    /**
     * Set CSV escape character.
     * @param escapeChar escape character
     * @return this writer for chaining
     */
    public CsvDatasetWriter escapeChar(char escapeChar) {
        this.escapeChar = escapeChar;
        return this;
    }

    /**
     * Set line ending string.
     * @param lineEnd line ending
     * @return this writer for chaining
     */
    public CsvDatasetWriter lineEnd(String lineEnd) {
        this.lineEnd = lineEnd;
        return this;
    }

    /**
     * Select specific fields to export by field names.
     * @param fieldNames field names to include
     * @return this writer for chaining
     */
    public CsvDatasetWriter fields(String... fieldNames) {
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
    public CsvDatasetWriter columns(String... columnNames) {
        if (metadata != null) {
            this.fieldSelector = FieldSelector.from(metadata).columns(columnNames);
        }
        return this;
    }

    /**
     * Select fields using generated field constants array.
     * @param fieldConstants array of field name constants
     * @return this writer for chaining
     */
    public CsvDatasetWriter fieldsArray(String[] fieldConstants) {
        if (metadata != null) {
            this.fieldSelector = FieldSelector.from(metadata).fieldsArray(fieldConstants);
        }
        return this;
    }

    /**
     * Select fields using generated column constants array.
     * @param columnConstants array of column name constants
     * @return this writer for chaining
     */
    public CsvDatasetWriter columnsArray(String[] columnConstants) {
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
    public CsvDatasetWriter exclude(String... fieldNames) {
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
    public CsvDatasetWriter requiredOnly() {
        if (metadata != null) {
            this.fieldSelector = FieldSelector.from(metadata).requiredOnly();
        }
        return this;
    }

    /**
     * Select only exportable fields (not ignored or hidden).
     * @return this writer for chaining
     */
    public CsvDatasetWriter exportableOnly() {
        if (metadata != null) {
            this.fieldSelector = FieldSelector.from(metadata).exportableOnly();
        }
        return this;
    }

    /**
     * Use pre-built metadata for field selection.
     * @param <T> record type
     * @param metadata POJO metadata
     * @return this writer for chaining
     */
    @SuppressWarnings("unchecked")
    public <T> CsvDatasetWriter select(PojoMetadata<T> metadata) {
        this.metadata = metadata;
        return this;
    }

    /**
     * Use custom field selector for advanced field selection.
     * @param <T> record type
     * @param selector field selector
     * @return this writer for chaining
     */
    @SuppressWarnings("unchecked")
    public <T> CsvDatasetWriter select(FieldSelector<T> selector) {
        this.fieldSelector = selector;
        return this;
    }

    /**
     * Write Dataset to CSV file.
     * @param <T> record type
     * @param dataset dataset to write
     * @throws IOException if file cannot be written
     */
    public <T> void write(Dataset<T> dataset) throws IOException {
        if (dataset.isEmpty()) {
            // Write empty file (with headers if applicable)
            try (CSVWriter writer = new CSVWriter(new FileWriter(filePath), separator, quoteChar, escapeChar, lineEnd)) {
                // nothing to write
            }
            return;
        }

        Class<?> recordClass = dataset.toList().get(0).getClass();
        if (!recordClass.isRecord()) {
            throw new IllegalArgumentException("Dataset must contain record types");
        }

        // Initialize metadata if not set
        if (metadata == null) {
            @SuppressWarnings("unchecked")
            Class<Object> typedClass = (Class<Object>) recordClass;
            metadata = MetadataCache.getMetadata(typedClass);
        }

        // Determine which fields to export
        List<FieldMeta> fieldsToExport;
        if (fieldSelector != null) {
            fieldsToExport = fieldSelector.select();
        } else if (selectedFields != null) {
            fieldsToExport = selectedFields;
        } else {
            fieldsToExport = metadata.getExportableFields();
        }

        if (fieldsToExport.isEmpty()) {
            throw new IllegalArgumentException("No fields selected for export");
        }

        try (CSVWriter writer = new CSVWriter(new FileWriter(filePath), separator, quoteChar, escapeChar, lineEnd)) {
            // Write headers
            if (includeHeaders) {
                String[] headers = fieldsToExport.stream()
                    .map(FieldMeta::getEffectiveColumnName)
                    .toArray(String[]::new);
                writer.writeNext(headers);
            }

            // Write data rows
            RecordComponent[] components = recordClass.getRecordComponents();
            for (T record : dataset.toList()) {
                String[] row = new String[fieldsToExport.size()];
                for (int i = 0; i < fieldsToExport.size(); i++) {
                    FieldMeta fieldMeta = fieldsToExport.get(i);
                    RecordComponent component = findComponent(components, fieldMeta.getFieldName());
                    if (component != null) {
                        try {
                            Object value = component.getAccessor().invoke(record);
                            row[i] = value != null ? value.toString() : "";
                        } catch (Exception e) {
                            row[i] = "";
                        }
                    } else {
                        row[i] = "";
                    }
                }
                writer.writeNext(row);
            }
        }
    }

    /**
     * Write an untyped {@link Table} to CSV.
     *
     * @param table the table to write
     * @throws IOException if file cannot be written
     */
    public void writeTable(Table table) throws IOException {
        try (CSVWriter writer = new CSVWriter(new FileWriter(filePath), separator, quoteChar, escapeChar, lineEnd)) {
            List<String> columns = table.columns();

            if (includeHeaders) {
                writer.writeNext(columns.toArray(new String[0]));
            }

            for (Map<String, CellValue> row : table.toList()) {
                String[] line = new String[columns.size()];
                for (int i = 0; i < columns.size(); i++) {
                    CellValue cv = row.get(columns.get(i));
                    if (cv == null || cv.isBlank()) {
                        line[i] = "";
                    } else {
                        line[i] = cv.asString();
                    }
                }
                writer.writeNext(line);
            }
        }
    }

    private RecordComponent findComponent(RecordComponent[] components, String fieldName) {
        for (RecordComponent component : components) {
            if (component.getName().equals(fieldName)) {
                return component;
            }
        }
        return null;
    }
}
