package dataset4j.poi;

import dataset4j.Dataset;
import dataset4j.annotations.*;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes multiple {@link Dataset} instances into a single Excel workbook,
 * one dataset per sheet.
 *
 * <p>Example:
 * <pre>{@code
 * ExcelWorkbookWriter.toFile("output.xlsx")
 *     .addSheet("SheetAScenario", aScenarioDataset)
 *     .addSheet("SheetA",         aResultsDataset)
 *     .addSheet("SheetBScenario", bScenarioDataset)
 *     .write();
 * }</pre>
 *
 * <p>Field selection defaults to all exportable fields derived from
 * {@code @DataColumn} annotations. Supply explicit field names to limit or
 * reorder columns for a specific sheet:
 * <pre>{@code
 * .addSheet("SheetA", aResultsDataset, AResults.Fields.ID, AResults.Fields.VALUE)
 * }</pre>
 *
 * <p>All sheets inherit the workbook-level {@link #headers(boolean)},
 * {@link #autoSize(boolean)}, {@link #cellWriter(CellWriter)}, and
 * {@link #defaultValue(Class, Object)} settings.
 */
public class ExcelWorkbookWriter {

    private final String filePath;
    private boolean defaultHeaders = true;
    private boolean defaultAutoSize = true;

    private CellWriter globalCellWriter;
    private final Map<String, CellWriter> fieldCellWriters = new HashMap<>();
    private final Map<Class<?>, Object> typeDefaults = new HashMap<>();
    private final Map<String, Object> fieldDefaults = new HashMap<>();

    private final List<SheetEntry<?>> sheets = new ArrayList<>();

    private ExcelWorkbookWriter(String filePath) {
        this.filePath = filePath;
    }

    /**
     * Create a writer targeting {@code filePath}.
     * @param filePath path to the output {@code .xlsx} file
     * @return a new writer instance
     */
    public static ExcelWorkbookWriter toFile(String filePath) {
        Path path = Paths.get(filePath).normalize();
        for (int i = 0; i < path.getNameCount(); i++) {
            if ("..".equals(path.getName(i).toString())) {
                throw new SecurityException("Path traversal detected in file path: " + filePath);
            }
        }
        return new ExcelWorkbookWriter(filePath);
    }

    /**
     * Whether to write a header row on every sheet (default: {@code true}).
     */
    public ExcelWorkbookWriter headers(boolean includeHeaders) {
        this.defaultHeaders = includeHeaders;
        return this;
    }

    /**
     * Whether to auto-size columns on every sheet (default: {@code true}).
     */
    public ExcelWorkbookWriter autoSize(boolean autoSize) {
        this.defaultAutoSize = autoSize;
        return this;
    }

    /**
     * Set a global custom cell writer applied to all cells across all sheets.
     * @param writer the cell writer
     * @return this writer for chaining
     */
    public ExcelWorkbookWriter cellWriter(CellWriter writer) {
        this.globalCellWriter = writer;
        return this;
    }

    /**
     * Set a custom cell writer for a specific field across all sheets.
     * Per-field writers take priority over the global writer.
     * @param fieldName the field name to target
     * @param writer the cell writer
     * @return this writer for chaining
     */
    public ExcelWorkbookWriter cellWriter(String fieldName, CellWriter writer) {
        this.fieldCellWriters.put(fieldName, writer);
        return this;
    }

    /**
     * Set a default value for all null fields of the given type during writing.
     * @param type the field type to configure
     * @param value the default value to write
     * @return this writer for chaining
     */
    public ExcelWorkbookWriter defaultValue(Class<?> type, Object value) {
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
    public ExcelWorkbookWriter defaultValue(String fieldName, Object value) {
        this.fieldDefaults.put(fieldName, value);
        return this;
    }

    /**
     * Add a sheet using all exportable fields defined by {@code @DataColumn} annotations.
     * @param sheetName name for the Excel sheet tab
     * @param dataset   data to write into this sheet
     */
    public <T> ExcelWorkbookWriter addSheet(String sheetName, Dataset<T> dataset) {
        sheets.add(new SheetEntry<>(sheetName, dataset, null));
        return this;
    }

    /**
     * Add a sheet with explicit field selection (varargs of field name constants).
     * @param sheetName  name for the Excel sheet tab
     * @param dataset    data to write into this sheet
     * @param fieldNames field name constants to include, in order
     *                   (e.g. {@code AScenario.Fields.ID, AScenario.Fields.NAME})
     */
    public <T> ExcelWorkbookWriter addSheet(String sheetName, Dataset<T> dataset, String... fieldNames) {
        sheets.add(new SheetEntry<>(sheetName, dataset, fieldNames));
        return this;
    }

    /**
     * Add a sheet with an array of field name constants (e.g. {@code AScenario.Fields.ALL_FIELDS}).
     * @param sheetName      name for the Excel sheet tab
     * @param dataset        data to write into this sheet
     * @param fieldNamesArray generated field constants array
     */
    public <T> ExcelWorkbookWriter addSheetFieldsArray(String sheetName, Dataset<T> dataset, String[] fieldNamesArray) {
        sheets.add(new SheetEntry<>(sheetName, dataset, fieldNamesArray));
        return this;
    }

    /**
     * Write all sheets to the workbook file.
     * @throws IOException if the file cannot be written
     * @throws IllegalStateException if no sheets were added
     */
    public void write() throws IOException {
        if (sheets.isEmpty()) {
            throw new IllegalStateException("No sheets added — call addSheet() before write()");
        }

        try (Workbook workbook = new XSSFWorkbook();
             FileOutputStream fos = new FileOutputStream(filePath)) {

            for (SheetEntry<?> entry : sheets) {
                renderEntry(workbook, entry);
            }

            workbook.write(fos);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> void renderEntry(Workbook workbook, SheetEntry<T> entry) {
        Dataset<T> dataset = entry.dataset();

        if (dataset.isEmpty()) {
            workbook.createSheet(entry.sheetName());
            return;
        }

        Class<?> rawClass = dataset.toList().get(0).getClass();
        if (!rawClass.isRecord()) {
            throw new IllegalArgumentException(
                    "Sheet '" + entry.sheetName() + "': dataset must contain record types");
        }

        PojoMetadata<T> metadata = MetadataCache.getMetadata((Class<T>) rawClass);

        List<FieldMeta> fieldsToExport;
        if (entry.fieldNames() != null && entry.fieldNames().length > 0) {
            fieldsToExport = FieldSelector.from(metadata).fieldsArray(entry.fieldNames()).select();
        } else {
            fieldsToExport = metadata.getExportableFields();
        }

        if (fieldsToExport.isEmpty()) {
            throw new IllegalArgumentException(
                    "Sheet '" + entry.sheetName() + "': no fields selected for export");
        }

        SheetRenderConfig config = new SheetRenderConfig(
                fieldsToExport,
                defaultHeaders,
                defaultAutoSize,
                globalCellWriter,
                fieldCellWriters,
                typeDefaults,
                fieldDefaults);

        ExcelSheetRenderer.renderSheet(workbook, entry.sheetName(), dataset, config);
    }

    // -------------------------------------------------------------------------

    private record SheetEntry<T>(String sheetName, Dataset<T> dataset, String[] fieldNames) {}
}
