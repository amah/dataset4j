package dataset4j.poi;

import dataset4j.Dataset;
import dataset4j.annotations.AnnotationProcessor;
import dataset4j.annotations.ColumnMetadata;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.RecordComponent;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads Excel files into Dataset using annotation-driven mapping.
 * 
 * Example usage:
 * {@code
 * Dataset<Employee> employees = ExcelDatasetReader
 *     .fromFile("employees.xlsx")
 *     .sheet("Employee Data")
 *     .readAs(Employee.class);
 * }
 */
public class ExcelDatasetReader {
    
    private final String filePath;
    private String sheetName;
    private boolean hasHeaders = true;
    private int startRow = 0;
    
    private ExcelDatasetReader(String filePath) {
        this.filePath = filePath;
    }
    
    /**
     * Create reader for Excel file.
     * @param filePath path to Excel file
     * @return new reader instance
     */
    public static ExcelDatasetReader fromFile(String filePath) {
        // Validate file path to prevent directory traversal attacks
        Path path = Paths.get(filePath).normalize();
        if (path.toString().contains("..")) {
            throw new SecurityException("Path traversal detected in file path: " + filePath);
        }
        return new ExcelDatasetReader(filePath);
    }
    
    /**
     * Specify sheet name to read from.
     * @param sheetName name of the sheet
     * @return this reader for chaining
     */
    public ExcelDatasetReader sheet(String sheetName) {
        this.sheetName = sheetName;
        return this;
    }
    
    /**
     * Specify whether first row contains headers.
     * @param hasHeaders true if first row has headers
     * @return this reader for chaining
     */
    public ExcelDatasetReader headers(boolean hasHeaders) {
        this.hasHeaders = hasHeaders;
        return this;
    }
    
    /**
     * Alias for headers() method.
     * @param hasHeaders true if first row has headers
     * @return this reader for chaining
     */
    public ExcelDatasetReader hasHeaders(boolean hasHeaders) {
        return headers(hasHeaders);
    }
    
    /**
     * Specify starting row index (0-based).
     * @param startRow row index to start reading from
     * @return this reader for chaining
     */
    public ExcelDatasetReader startRow(int startRow) {
        this.startRow = startRow;
        return this;
    }
    
    /**
     * Read Excel data into Dataset of specified record type.
     * @param <T> record type
     * @param recordClass record class with @DataColumn annotations
     * @return Dataset containing parsed records
     * @throws IOException if file cannot be read
     */
    public <T> Dataset<T> read(Class<T> recordClass) throws IOException {
        return readAs(recordClass);
    }
    
    /**
     * Read Excel data into Dataset of specified record type.
     * @param <T> record type
     * @param recordClass record class with @DataColumn annotations
     * @return Dataset containing parsed records
     * @throws IOException if file cannot be read
     */
    public <T> Dataset<T> readAs(Class<T> recordClass) throws IOException {
        if (!recordClass.isRecord()) {
            throw new IllegalArgumentException("Class must be a record: " + recordClass.getName());
        }
        
        List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(recordClass);
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("Record must have @DataColumn annotations");
        }
        
        try (FileInputStream fis = new FileInputStream(new File(filePath));
             Workbook workbook = new XSSFWorkbook(fis)) {
            
            Sheet sheet = sheetName != null ? workbook.getSheet(sheetName) : workbook.getSheetAt(0);
            if (sheet == null) {
                throw new IllegalArgumentException("Sheet not found: " + sheetName);
            }
            
            List<T> records = new ArrayList<>();
            int dataStartRow = hasHeaders ? startRow + 1 : startRow;
            
            for (int rowIndex = dataStartRow; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                Row row = sheet.getRow(rowIndex);
                if (row == null || isRowEmpty(row)) {
                    continue;
                }
                
                T record = parseRowToRecord(row, recordClass, columns);
                if (record != null) {
                    records.add(record);
                }
            }
            
            return Dataset.of(records);
        }
    }
    
    private boolean isRowEmpty(Row row) {
        for (int i = row.getFirstCellNum(); i < row.getLastCellNum(); i++) {
            Cell cell = row.getCell(i);
            if (cell != null && cell.getCellType() != CellType.BLANK) {
                String value = getCellValueAsString(cell);
                if (!value.trim().isEmpty()) {
                    return false;
                }
            }
        }
        return true;
    }
    
    private <T> T parseRowToRecord(Row row, Class<T> recordClass, List<ColumnMetadata> columns) {
        try {
            RecordComponent[] components = recordClass.getRecordComponents();
            Object[] values = new Object[components.length];
            
            for (int i = 0; i < components.length; i++) {
                RecordComponent component = components[i];
                ColumnMetadata columnMeta = findColumnForComponent(component, columns);
                
                if (columnMeta != null && !columnMeta.isIgnored()) {
                    int colIndex = getColumnIndex(columnMeta);
                    Cell cell = row.getCell(colIndex);
                    values[i] = parseCellValue(cell, component.getType(), columnMeta);
                } else {
                    values[i] = getDefaultValue(component.getType());
                }
            }
            
            return recordClass.getDeclaredConstructor(
                java.util.Arrays.stream(components)
                    .map(RecordComponent::getType)
                    .toArray(Class[]::new)
            ).newInstance(values);
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to create record from row", e);
        }
    }
    
    private ColumnMetadata findColumnForComponent(RecordComponent component, List<ColumnMetadata> columns) {
        return columns.stream()
            .filter(col -> col.getFieldName().equals(component.getName()))
            .findFirst()
            .orElse(null);
    }
    
    private int getColumnIndex(ColumnMetadata columnMeta) {
        // Use explicit column index if specified, otherwise use order
        return columnMeta.getOrder() >= 0 ? columnMeta.getOrder() : 0;
    }
    
    private Object parseCellValue(Cell cell, Class<?> targetType, ColumnMetadata columnMeta) {
        if (cell == null || cell.getCellType() == CellType.BLANK) {
            return getDefaultValue(targetType);
        }
        
        String cellValue = getCellValueAsString(cell);
        if (cellValue.trim().isEmpty()) {
            return getDefaultValue(targetType);
        }
        
        // Use FormatProvider for parsing if available
        try {
            return dataset4j.annotations.FormatProvider.parseValue(cellValue, columnMeta);
        } catch (Exception e) {
            // Fallback to basic parsing
            return parseBasicValue(cellValue, targetType);
        }
    }
    
    private String getCellValueAsString(Cell cell) {
        return switch (cell.getCellType()) {
            case STRING -> cell.getStringCellValue();
            case NUMERIC -> {
                if (DateUtil.isCellDateFormatted(cell)) {
                    yield cell.getDateCellValue().toString();
                } else {
                    double numValue = cell.getNumericCellValue();
                    if (numValue == (long) numValue) {
                        yield String.valueOf((long) numValue);
                    } else {
                        yield String.valueOf(numValue);
                    }
                }
            }
            case BOOLEAN -> String.valueOf(cell.getBooleanCellValue());
            case FORMULA -> {
                try {
                    FormulaEvaluator evaluator = cell.getSheet().getWorkbook().getCreationHelper().createFormulaEvaluator();
                    CellValue cellValue = evaluator.evaluate(cell);
                    yield switch (cellValue.getCellType()) {
                        case NUMERIC -> String.valueOf(cellValue.getNumberValue());
                        case STRING -> cellValue.getStringValue();
                        case BOOLEAN -> String.valueOf(cellValue.getBooleanValue());
                        default -> "";
                    };
                } catch (Exception e) {
                    yield ""; // Fallback for formula evaluation errors
                }
            }
            default -> "";
        };
    }
    
    private Object parseBasicValue(String value, Class<?> type) {
        if (type == String.class) return value;
        if (type == int.class || type == Integer.class) return Integer.parseInt(value);
        if (type == long.class || type == Long.class) return Long.parseLong(value);
        if (type == double.class || type == Double.class) return Double.parseDouble(value);
        if (type == boolean.class || type == Boolean.class) return Boolean.parseBoolean(value);
        return value;
    }
    
    private Object getDefaultValue(Class<?> type) {
        if (type == String.class) return "";
        if (type == int.class) return 0;
        if (type == long.class) return 0L;
        if (type == double.class) return 0.0;
        if (type == boolean.class) return false;
        return null;
    }
}