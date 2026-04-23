package dataset4j.poi;

import dataset4j.CellValue;
import dataset4j.Dataset;
import dataset4j.DatasetReadException;
import dataset4j.Table;
import dataset4j.ValueType;
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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads Excel files into Dataset using annotation-driven mapping.
 *
 * <p>Columns can be mapped by position ({@code order}) or by header name ({@code name}).
 * When {@code order} is not specified, the reader matches columns by header name.
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

    // Configurable default values
    private final Map<Class<?>, Object> typeDefaults = new LinkedHashMap<>();
    private final Map<String, Object> fieldDefaults = new LinkedHashMap<>();

    private FormulaErrorHandler formulaErrorHandler = FormulaErrorHandler.BLANK;

    // Per-read scratch: a single evaluator is created once and reused for every formula cell
    // (creating one per cell is O(n) × workbook-rebuild cost and was a major perf sink).
    private FormulaEvaluator currentEvaluator;

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
     * Set a default value for all fields of the given type when cells are blank, empty, or null.
     * @param type the field type to configure
     * @param value the default value to use
     * @return this reader for chaining
     */
    public ExcelDatasetReader defaultValue(Class<?> type, Object value) {
        this.typeDefaults.put(type, value);
        return this;
    }

    /**
     * Set a default value for a specific field when its cell is blank, empty, or null.
     * Per-field defaults take priority over type-based defaults.
     * @param fieldName the record field name
     * @param value the default value to use
     * @return this reader for chaining
     */
    public ExcelDatasetReader defaultValue(String fieldName, Object value) {
        this.fieldDefaults.put(fieldName, value);
        return this;
    }

    /**
     * Install a handler invoked when a formula cell cannot be evaluated (or its cached
     * result is an Excel error, e.g. {@code #DIV/0!}, {@code #N/A}).
     *
     * <p>The default is {@link FormulaErrorHandler#BLANK}, which treats the cell as blank
     * and falls back to the field's configured default value.
     *
     * @param handler handler to invoke; if {@code null}, reverts to {@link FormulaErrorHandler#BLANK}
     * @return this reader for chaining
     */
    public ExcelDatasetReader onFormulaError(FormulaErrorHandler handler) {
        this.formulaErrorHandler = handler != null ? handler : FormulaErrorHandler.BLANK;
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

            String resolvedSheetName = sheet.getSheetName();
            this.currentEvaluator = workbook.getCreationHelper().createFormulaEvaluator();

            try {
                // Build header index map for name-based column matching
                Map<String, Integer> headerIndex = buildHeaderIndex(sheet);

                List<T> records = new ArrayList<>();
                int dataStartRow = hasHeaders ? startRow + 1 : startRow;

                for (int rowIndex = dataStartRow; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                    Row row = sheet.getRow(rowIndex);
                    if (row == null || isRowEmpty(row)) {
                        continue;
                    }

                    T record = parseRowToRecord(row, rowIndex, recordClass, columns, headerIndex, resolvedSheetName);
                    if (record != null) {
                        records.add(record);
                    }
                }

                return Dataset.of(records);
            } finally {
                this.currentEvaluator = null;
            }
        }
    }

    /**
     * Read Excel data into an untyped {@link Table}, preserving cell types and format strings.
     *
     * <p>Column names are derived from the header row (if {@code hasHeaders} is true)
     * or generated as "Column1", "Column2", etc.
     *
     * @return a Table with CellValues preserving source type and format info
     * @throws IOException if the file cannot be read
     */
    public Table readTable() throws IOException {
        try (FileInputStream fis = new FileInputStream(new File(filePath));
             Workbook workbook = new XSSFWorkbook(fis)) {

            Sheet sheet = sheetName != null ? workbook.getSheet(sheetName) : workbook.getSheetAt(0);
            if (sheet == null) {
                throw new IllegalArgumentException("Sheet not found: " + sheetName);
            }

            // Determine column names from header row or generate them
            List<String> columnNames;
            int dataStartRow;

            if (hasHeaders) {
                Row headerRow = sheet.getRow(startRow);
                if (headerRow == null) {
                    return Table.empty();
                }
                columnNames = new ArrayList<>();
                for (int i = headerRow.getFirstCellNum(); i < headerRow.getLastCellNum(); i++) {
                    Cell cell = headerRow.getCell(i);
                    String name = (cell != null) ? getCellValueAsString(cell, String.class).trim() : "";
                    columnNames.add(name.isEmpty() ? "Column" + (i + 1) : name);
                }
                dataStartRow = startRow + 1;
            } else {
                // Scan first data row to determine column count
                Row firstRow = sheet.getRow(startRow);
                if (firstRow == null) {
                    return Table.empty();
                }
                int colCount = firstRow.getLastCellNum() - firstRow.getFirstCellNum();
                columnNames = new ArrayList<>();
                for (int i = 0; i < colCount; i++) {
                    columnNames.add("Column" + (i + 1));
                }
                dataStartRow = startRow;
            }

            List<Map<String, CellValue>> rows = new ArrayList<>();
            int firstCellNum = hasHeaders ? sheet.getRow(startRow).getFirstCellNum() : sheet.getRow(startRow).getFirstCellNum();

            for (int rowIndex = dataStartRow; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                Row row = sheet.getRow(rowIndex);
                if (row == null || isRowEmpty(row)) {
                    continue;
                }

                Map<String, CellValue> rowMap = new LinkedHashMap<>();
                for (int c = 0; c < columnNames.size(); c++) {
                    int cellIndex = firstCellNum + c;
                    Cell cell = row.getCell(cellIndex);
                    rowMap.put(columnNames.get(c), extractCellValue(cell));
                }
                rows.add(rowMap);
            }

            return Table.of(columnNames, rows);
        }
    }

    /**
     * Extract a CellValue from an Excel cell, preserving the cell type and format string.
     */
    private CellValue extractCellValue(Cell cell) {
        if (cell == null) {
            return CellValue.blank();
        }

        String formatString = null;
        CellStyle style = cell.getCellStyle();
        if (style != null) {
            String fmt = style.getDataFormatString();
            if (fmt != null && !"General".equals(fmt)) {
                formatString = fmt;
            }
        }

        return switch (cell.getCellType()) {
            case STRING -> CellValue.of(cell.getStringCellValue(), ValueType.STRING, formatString);
            case NUMERIC -> {
                if (DateUtil.isCellDateFormatted(cell)) {
                    LocalDateTime ldt = cell.getLocalDateTimeCellValue();
                    // If time component is midnight, treat as DATE; otherwise DATETIME
                    if (ldt.getHour() == 0 && ldt.getMinute() == 0 && ldt.getSecond() == 0 && ldt.getNano() == 0) {
                        yield CellValue.of(ldt.toLocalDate(), ValueType.DATE, formatString);
                    } else {
                        yield CellValue.of(ldt, ValueType.DATETIME, formatString);
                    }
                } else {
                    double numValue = cell.getNumericCellValue();
                    // Store as integer if it's a whole number
                    if (numValue == (long) numValue && !Double.isInfinite(numValue)) {
                        yield CellValue.of((long) numValue, ValueType.NUMBER, formatString);
                    } else {
                        yield CellValue.of(numValue, ValueType.NUMBER, formatString);
                    }
                }
            }
            case BOOLEAN -> CellValue.of(cell.getBooleanCellValue(), ValueType.BOOLEAN, formatString);
            case FORMULA -> {
                // Store the formula expression; also try to evaluate
                String formula = cell.getCellFormula();
                yield CellValue.of(formula, ValueType.FORMULA, formatString);
            }
            case ERROR -> CellValue.error(String.valueOf(cell.getErrorCellValue()));
            case BLANK -> CellValue.blank();
            default -> CellValue.blank();
        };
    }

    private Map<String, Integer> buildHeaderIndex(Sheet sheet) {
        Map<String, Integer> index = new LinkedHashMap<>();
        if (!hasHeaders) {
            return index;
        }
        Row headerRow = sheet.getRow(startRow);
        if (headerRow == null) {
            return index;
        }
        for (int i = headerRow.getFirstCellNum(); i < headerRow.getLastCellNum(); i++) {
            Cell cell = headerRow.getCell(i);
            if (cell != null) {
                String value = getCellValueAsString(cell, String.class).trim();
                if (!value.isEmpty()) {
                    index.put(value, i);
                }
            }
        }
        return index;
    }

    private boolean isRowEmpty(Row row) {
        for (int i = row.getFirstCellNum(); i < row.getLastCellNum(); i++) {
            Cell cell = row.getCell(i);
            if (cell != null && cell.getCellType() != CellType.BLANK) {
                String value = getCellValueAsString(cell, String.class);
                if (!value.trim().isEmpty()) {
                    return false;
                }
            }
        }
        return true;
    }

    private <T> T parseRowToRecord(Row row, int rowIndex, Class<T> recordClass,
                                    List<ColumnMetadata> columns, Map<String, Integer> headerIndex,
                                    String resolvedSheetName) {
        try {
            RecordComponent[] components = recordClass.getRecordComponents();
            Object[] values = new Object[components.length];

            for (int i = 0; i < components.length; i++) {
                RecordComponent component = components[i];
                ColumnMetadata columnMeta = findColumnForComponent(component, columns);

                if (columnMeta != null && !columnMeta.isIgnored()) {
                    int colIndex = resolveColumnIndex(columnMeta, headerIndex);
                    if (colIndex < 0) {
                        values[i] = resolveDefault(component.getType(), columnMeta);
                        continue;
                    }
                    Cell cell = row.getCell(colIndex);
                    try {
                        values[i] = parseCellValue(cell, component.getType(), columnMeta);
                    } catch (Exception e) {
                        String rawValue = cell != null ? getCellValueAsString(cell, component.getType()) : null;
                        throw DatasetReadException.builder()
                            .row(rowIndex)
                            .column(colIndex)
                            .sheetName(resolvedSheetName)
                            .fieldName(columnMeta.getFieldName())
                            .recordClass(recordClass)
                            .rawValue(rawValue)
                            .fieldTypeName(component.getType().getSimpleName())
                            .parseMessage(e.getMessage())
                            .cause(e)
                            .build();
                    }
                } else {
                    values[i] = resolveDefault(component.getType(), columnMeta);
                }
            }

            return recordClass.getDeclaredConstructor(
                java.util.Arrays.stream(components)
                    .map(RecordComponent::getType)
                    .toArray(Class[]::new)
            ).newInstance(values);

        } catch (DatasetReadException e) {
            throw e;
        } catch (Exception e) {
            throw new DatasetReadException.Builder()
                .row(rowIndex)
                .sheetName(resolvedSheetName)
                .recordClass(recordClass)
                .parseMessage("Failed to create record from row: " + e.getMessage())
                .cause(e)
                .build();
        }
    }

    /**
     * Resolve column index: use explicit order if set, otherwise match by header name.
     */
    private int resolveColumnIndex(ColumnMetadata columnMeta, Map<String, Integer> headerIndex) {
        // If order is explicitly set, use it (1-based to 0-based)
        if (columnMeta.getOrder() > 0) {
            return columnMeta.getOrder() - 1;
        }

        // Otherwise, match by effective column name against headers
        String effectiveName = columnMeta.getEffectiveColumnName();
        Integer idx = headerIndex.get(effectiveName);
        if (idx != null) {
            return idx;
        }

        // Case-insensitive fallback
        for (Map.Entry<String, Integer> entry : headerIndex.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(effectiveName)) {
                return entry.getValue();
            }
        }

        return -1; // column not found — will use default value
    }

    private ColumnMetadata findColumnForComponent(RecordComponent component, List<ColumnMetadata> columns) {
        return columns.stream()
            .filter(col -> col.getFieldName().equals(component.getName()))
            .findFirst()
            .orElse(null);
    }

    private Object parseCellValue(Cell cell, Class<?> targetType, ColumnMetadata columnMeta) {
        if (cell == null || cell.getCellType() == CellType.BLANK) {
            return resolveDefault(targetType, columnMeta);
        }

        String cellValue = getCellValueAsString(cell, targetType);
        if (cellValue.trim().isEmpty()) {
            return resolveDefault(targetType, columnMeta);
        }

        // Use FormatProvider for parsing if available
        try {
            Object result = dataset4j.annotations.FormatProvider.parseValue(cellValue, columnMeta);
            // If FormatProvider returned a String but we need something else, try basic parsing
            if (result instanceof String && targetType != String.class) {
                return parseBasicValue(cellValue, targetType);
            }
            return result;
        } catch (Exception e) {
            // Fallback to basic parsing
            return parseBasicValue(cellValue, targetType);
        }
    }

    private String getCellValueAsString(Cell cell, Class<?> targetType) {
        return switch (cell.getCellType()) {
            case STRING -> cell.getStringCellValue();
            case NUMERIC -> {
                if (DateUtil.isCellDateFormatted(cell)) {
                    LocalDateTime ldt = cell.getLocalDateTimeCellValue();
                    yield targetType == LocalDateTime.class ? ldt.toString() : ldt.toLocalDate().toString();
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
            case FORMULA -> evaluateFormula(cell, targetType);
            default -> "";
        };
    }

    /**
     * Resolve a formula cell's value as a String.
     *
     * <p>Tries the cached result first (Excel writes the last-computed value to the file,
     * so no re-evaluation is needed in the common case). Falls back to a single reused
     * {@link FormulaEvaluator} if the cached value cannot be read. Any failure — or a
     * cached {@code ERROR} — is routed through {@link #formulaErrorHandler}.
     */
    private String evaluateFormula(Cell cell, Class<?> targetType) {
        try {
            CellType cached = cell.getCachedFormulaResultType();
            return switch (cached) {
                case NUMERIC -> {
                    if (DateUtil.isCellDateFormatted(cell)) {
                        LocalDateTime ldt = cell.getLocalDateTimeCellValue();
                        yield targetType == LocalDateTime.class ? ldt.toString() : ldt.toLocalDate().toString();
                    }
                    double numValue = cell.getNumericCellValue();
                    yield numValue == (long) numValue
                        ? String.valueOf((long) numValue)
                        : String.valueOf(numValue);
                }
                case STRING -> cell.getStringCellValue();
                case BOOLEAN -> String.valueOf(cell.getBooleanCellValue());
                case ERROR -> nullToEmpty(formulaErrorHandler.handle(cell, null));
                default -> "";
            };
        } catch (Exception cachedFailed) {
            // Cached value unreadable — try a live evaluation with the reused evaluator.
            try {
                FormulaEvaluator evaluator = currentEvaluator;
                if (evaluator == null) {
                    evaluator = cell.getSheet().getWorkbook().getCreationHelper().createFormulaEvaluator();
                }
                org.apache.poi.ss.usermodel.CellValue cellValue = evaluator.evaluate(cell);
                return switch (cellValue.getCellType()) {
                    case NUMERIC -> String.valueOf(cellValue.getNumberValue());
                    case STRING -> cellValue.getStringValue();
                    case BOOLEAN -> String.valueOf(cellValue.getBooleanValue());
                    case ERROR -> nullToEmpty(formulaErrorHandler.handle(cell, null));
                    default -> "";
                };
            } catch (Exception evalFailed) {
                return nullToEmpty(formulaErrorHandler.handle(cell, evalFailed));
            }
        }
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private Object parseBasicValue(String value, Class<?> type) {
        if (type == String.class) return value;
        if (type == int.class || type == Integer.class) return Integer.parseInt(value);
        if (type == long.class || type == Long.class) return Long.parseLong(value);
        if (type == double.class || type == Double.class) return Double.parseDouble(value);
        if (type == boolean.class || type == Boolean.class) return Boolean.parseBoolean(value);
        if (type == java.math.BigDecimal.class) return new java.math.BigDecimal(value);
        if (type == java.time.LocalDate.class) {
            try {
                // First try to parse as ISO date
                return java.time.LocalDate.parse(value, java.time.format.DateTimeFormatter.ISO_LOCAL_DATE);
            } catch (Exception e) {
                // If that fails, try to parse as Excel serial date number
                try {
                    double serialDate = Double.parseDouble(value);
                    // Excel epoch is January 1, 1900, but has a leap year bug (treats 1900 as leap year)
                    // Use POI's utility to convert Excel serial date to LocalDate
                    java.util.Date date = org.apache.poi.ss.usermodel.DateUtil.getJavaDate(serialDate);
                    LocalDate localDate = date.toInstant().atZone(java.time.ZoneId.systemDefault()).toLocalDate();

                    // Workaround for Excel 1900 leap year bug
                    // Excel incorrectly treats 1900 as a leap year, which shifts dates
                    // For dates on or before 1900-02-28, we need to add one day
                    if (serialDate <= 60) { // 60 is March 1, 1900 in Excel
                        localDate = localDate.plusDays(1);
                    }

                    return localDate;
                } catch (Exception ex) {
                    throw new IllegalArgumentException("Could not parse date: " + value, ex);
                }
            }
        }
        if (type == java.time.LocalDateTime.class) {
            return java.time.LocalDateTime.parse(value, java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        }
        if (type == java.time.OffsetDateTime.class) {
            return java.time.OffsetDateTime.parse(value);
        }
        if (type == java.time.ZonedDateTime.class) {
            return java.time.ZonedDateTime.parse(value);
        }
        return value;
    }

    /**
     * Resolve the default value using priority chain:
     * 1. Per-field override (fieldDefaults)
     * 2. Type-based override (typeDefaults)
     * 3. @DataColumn(defaultValue) annotation
     * 4. Built-in hardcoded defaults
     */
    private Object resolveDefault(Class<?> targetType, ColumnMetadata columnMeta) {
        // 1. Per-field override
        if (columnMeta != null && fieldDefaults.containsKey(columnMeta.getFieldName())) {
            return fieldDefaults.get(columnMeta.getFieldName());
        }

        // 2. Type-based override
        if (typeDefaults.containsKey(targetType)) {
            return typeDefaults.get(targetType);
        }

        // 3. @DataColumn(defaultValue) annotation
        if (columnMeta != null && !columnMeta.getDefaultValue().isEmpty()) {
            return parseBasicValue(columnMeta.getDefaultValue(), targetType);
        }

        // 4. Built-in hardcoded defaults
        return getDefaultValue(targetType);
    }

    private Object getDefaultValue(Class<?> type) {
        if (type == String.class) return null;
        if (type == int.class) return 0;
        if (type == long.class) return 0L;
        if (type == double.class) return 0.0;
        if (type == boolean.class) return false;
        if (type == java.math.BigDecimal.class) return java.math.BigDecimal.ZERO;
        if (type == java.time.LocalDate.class) return null;
        if (type == java.time.LocalDateTime.class) return null;
        return null;
    }
}
