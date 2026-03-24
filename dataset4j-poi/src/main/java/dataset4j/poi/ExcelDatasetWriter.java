package dataset4j.poi;

import dataset4j.Dataset;
import dataset4j.annotations.*;
import dataset4j.annotations.DataColumn;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.RecordComponent;
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
    
    private ExcelDatasetWriter(String filePath) {
        this.filePath = filePath;
    }
    
    /**
     * Create writer for Excel file.
     * @param filePath path to output Excel file
     * @return new writer instance
     */
    public static ExcelDatasetWriter toFile(String filePath) {
        // Validate file path to prevent directory traversal attacks
        Path path = Paths.get(filePath).normalize();
        if (path.toString().contains("..")) {
            throw new SecurityException("Path traversal detected in file path: " + filePath);
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
        // Handle empty dataset - just create an empty Excel file
        if (dataset.isEmpty()) {
            try (Workbook workbook = new XSSFWorkbook();
                 FileOutputStream fos = new FileOutputStream(filePath)) {
                workbook.createSheet(sheetName);
                workbook.write(fos);
            }
            return;
        }
        
        Class<?> recordClass = dataset.toList().get(0).getClass();
        if (!recordClass.isRecord()) {
            throw new IllegalArgumentException("Dataset must contain record types");
        }
        
        // Get or create metadata
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
        
        // Convert to ColumnMetadata for backward compatibility
        List<ColumnMetadata> columns = fieldsToExport.stream()
                .map(FieldMeta::toColumnMetadata)
                .collect(java.util.stream.Collectors.toList());
        
        try (Workbook workbook = new XSSFWorkbook();
             FileOutputStream fos = new FileOutputStream(filePath)) {
            
            Sheet sheet = workbook.createSheet(sheetName);
            CellStyle headerStyle = createHeaderStyle(workbook);
            Map<String, CellStyle> columnStyles = buildColumnStyles(fieldsToExport, workbook);

            int rowIndex = 0;

            // Write headers
            if (includeHeaders) {
                Row headerRow = sheet.createRow(rowIndex++);
                writeHeaders(headerRow, fieldsToExport, headerStyle);
            }

            // Write data rows
            for (T record : dataset.toList()) {
                Row dataRow = sheet.createRow(rowIndex++);
                writeDataRow(dataRow, record, fieldsToExport, columnStyles, workbook);
            }
            
            // Apply column formatting
            applyColumnFormatting(sheet, fieldsToExport, workbook);
            
            // Auto-size columns if requested
            if (autoSizeColumns) {
                for (int i = 0; i < fieldsToExport.size(); i++) {
                    sheet.autoSizeColumn(i);
                }
            }
            
            workbook.write(fos);
        }
    }
    
    private void writeHeaders(Row headerRow, List<FieldMeta> fields, CellStyle headerStyle) {
        for (int i = 0; i < fields.size(); i++) {
            FieldMeta field = fields.get(i);
            Cell cell = headerRow.createCell(i);
            cell.setCellValue(field.getEffectiveColumnName());
            cell.setCellStyle(headerStyle);
        }
    }
    
    private <T> void writeDataRow(Row row, T record, List<FieldMeta> fields,
                                   Map<String, CellStyle> columnStyles, Workbook workbook) {
        RecordComponent[] components = record.getClass().getRecordComponents();

        for (int i = 0; i < fields.size(); i++) {
            FieldMeta fieldMeta = fields.get(i);
            Cell cell = row.createCell(i);

            RecordComponent component = findComponent(components, fieldMeta.getFieldName());
            if (component != null) {
                try {
                    Object value = component.getAccessor().invoke(record);
                    CellWriter writer = resolveWriter(fieldMeta.getFieldName());
                    CellWriterContext context = new CellWriterContext(
                            cell, value, fieldMeta, workbook,
                            columnStyles.get(fieldMeta.getFieldName()),
                            DefaultCellWriter.INSTANCE);
                    writer.write(context);
                } catch (Exception e) {
                    System.err.printf("Warning: Failed to extract value from field '%s' in row %d: %s%n",
                            fieldMeta.getFieldName(), row.getRowNum(), e.getMessage());
                    cell.setCellValue("");
                }
            }
        }
    }

    private CellWriter resolveWriter(String fieldName) {
        CellWriter perField = fieldCellWriters.get(fieldName);
        if (perField != null) return perField;
        if (globalCellWriter != null) return globalCellWriter;
        return DefaultCellWriter.INSTANCE;
    }
    
    private RecordComponent findComponent(RecordComponent[] components, String fieldName) {
        for (RecordComponent component : components) {
            if (component.getName().equals(fieldName)) {
                return component;
            }
        }
        return null;
    }
    
    private Map<String, CellStyle> buildColumnStyles(List<FieldMeta> fields, Workbook workbook) {
        DataFormat dataFormat = workbook.createDataFormat();
        Map<String, CellStyle> styles = new HashMap<>();

        for (FieldMeta fieldMeta : fields) {
            CellStyle style = workbook.createCellStyle();

            // Number format
            if (!fieldMeta.getNumberFormat().isEmpty()) {
                style.setDataFormat(dataFormat.getFormat(fieldMeta.getNumberFormat()));
            }

            // Date format for date/datetime fields
            Class<?> fieldType = fieldMeta.getFieldType();
            if (fieldType == java.time.LocalDate.class || fieldType == java.time.LocalDateTime.class) {
                String dateFormat = fieldMeta.getDateFormat();
                if (fieldType == java.time.LocalDateTime.class && dateFormat.equals("yyyy-MM-dd")) {
                    dateFormat = "yyyy-MM-dd HH:mm:ss";
                }
                String excelFormat = JavaToExcelDateFormat.convert(dateFormat);
                style.setDataFormat(dataFormat.getFormat(excelFormat));
            }

            // Font (only create when needed)
            if (fieldMeta.isBold() || !fieldMeta.getFontColor().isEmpty()) {
                Font font = workbook.createFont();
                if (fieldMeta.isBold()) {
                    font.setBold(true);
                }
                if (!fieldMeta.getFontColor().isEmpty()) {
                    try {
                        font.setColor(getColorIndex(fieldMeta.getFontColor()));
                    } catch (Exception e) {
                        // Ignore invalid colors
                    }
                }
                style.setFont(font);
            }

            // Background color
            if (!fieldMeta.getBackgroundColor().isEmpty()) {
                try {
                    style.setFillForegroundColor(getColorIndex(fieldMeta.getBackgroundColor()));
                    style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
                } catch (Exception e) {
                    // Ignore invalid colors
                }
            }

            // Text wrapping
            if (fieldMeta.isWrapText()) {
                style.setWrapText(true);
            }

            // Alignment
            switch (fieldMeta.getAlignment()) {
                case LEFT   -> style.setAlignment(HorizontalAlignment.LEFT);
                case CENTER -> style.setAlignment(HorizontalAlignment.CENTER);
                case RIGHT  -> style.setAlignment(HorizontalAlignment.RIGHT);
                case AUTO   -> style.setAlignment(
                        Number.class.isAssignableFrom(fieldType)
                                ? HorizontalAlignment.RIGHT
                                : HorizontalAlignment.LEFT);
            }

            styles.put(fieldMeta.getFieldName(), style);
        }
        return styles;
    }
    
    private void applyColumnFormatting(Sheet sheet, List<FieldMeta> fields, Workbook workbook) {
        int freezeColumn = -1;
        
        for (int i = 0; i < fields.size(); i++) {
            FieldMeta field = fields.get(i);
            
            // Set column width if specified
            if (field.getWidth() > 0) {
                sheet.setColumnWidth(i, field.getWidth() * 256); // POI uses 1/256th units
            }
            
            // Track frozen columns
            if (field.isFrozen() && freezeColumn < i) {
                freezeColumn = i;
            }
        }
        
        // Apply freeze panes for rightmost frozen column
        if (freezeColumn >= 0) {
            sheet.createFreezePane(freezeColumn + 1, includeHeaders ? 1 : 0);
        }
    }
    
    private CellStyle createHeaderStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        
        // Bold font
        Font font = workbook.createFont();
        font.setBold(true);
        style.setFont(font);
        
        // Background color
        style.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        
        // Borders
        style.setBorderTop(BorderStyle.THIN);
        style.setBorderBottom(BorderStyle.THIN);
        style.setBorderLeft(BorderStyle.THIN);
        style.setBorderRight(BorderStyle.THIN);
        
        return style;
    }
    
    /**
     * Convert color name or hex to POI color index.
     * This is a simplified implementation - a full implementation would support more colors.
     */
    private short getColorIndex(String color) {
        if (color.startsWith("#")) {
            // Hex colors - simplified mapping to closest indexed color
            return IndexedColors.AUTOMATIC.getIndex();
        }
        
        // Named colors
        switch (color.toLowerCase()) {
            case "red": return IndexedColors.RED.getIndex();
            case "blue": return IndexedColors.BLUE.getIndex();
            case "green": return IndexedColors.GREEN.getIndex();
            case "yellow": return IndexedColors.YELLOW.getIndex();
            case "orange": return IndexedColors.ORANGE.getIndex();
            case "gray": case "grey": return IndexedColors.GREY_25_PERCENT.getIndex();
            case "black": return IndexedColors.BLACK.getIndex();
            case "white": return IndexedColors.WHITE.getIndex();
            default: return IndexedColors.AUTOMATIC.getIndex();
        }
    }
}