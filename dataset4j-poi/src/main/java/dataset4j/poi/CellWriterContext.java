package dataset4j.poi;

import dataset4j.annotations.FieldMeta;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Workbook;

/**
 * Context object passed to {@link CellWriter} implementations.
 * Bundles the POI cell, the extracted field value, column metadata,
 * the workbook (for creating styles/fonts), and the pre-built annotation-driven style.
 */
public final class CellWriterContext {

    private final Cell cell;
    private final Object value;
    private final FieldMeta fieldMeta;
    private final Workbook workbook;
    private final CellStyle columnStyle;
    private final CellWriter defaultWriter;

    CellWriterContext(Cell cell, Object value, FieldMeta fieldMeta,
                      Workbook workbook, CellStyle columnStyle, CellWriter defaultWriter) {
        this.cell = cell;
        this.value = value;
        this.fieldMeta = fieldMeta;
        this.workbook = workbook;
        this.columnStyle = columnStyle;
        this.defaultWriter = defaultWriter;
    }

    /** The already-created POI cell. */
    public Cell getCell() {
        return cell;
    }

    /** The extracted field value (may be null). */
    public Object getValue() {
        return value;
    }

    /** Column metadata from annotations. */
    public FieldMeta getFieldMeta() {
        return fieldMeta;
    }

    /** The workbook, useful for creating styles, fonts, or hyperlinks. */
    public Workbook getWorkbook() {
        return workbook;
    }

    /** The pre-built annotation-driven cell style. */
    public CellStyle getColumnStyle() {
        return columnStyle;
    }

    /**
     * Delegates to the built-in default cell-writing logic.
     * Custom writers can call this as a fallback for cases they don't handle.
     */
    public void writeDefault() {
        defaultWriter.write(this);
    }
}
