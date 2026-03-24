package dataset4j.poi;

import org.apache.poi.ss.usermodel.Cell;

/**
 * Default cell-writing logic extracted from the original {@code setCellValue} in
 * {@link ExcelDatasetWriter}. Handles type dispatch (Number, Boolean, Date, etc.)
 * and applies the annotation-driven column style.
 */
final class DefaultCellWriter implements CellWriter {

    static final DefaultCellWriter INSTANCE = new DefaultCellWriter();

    private DefaultCellWriter() {}

    @Override
    public void write(CellWriterContext context) {
        Cell cell = context.getCell();
        Object value = context.getValue();

        if (value == null) {
            cell.setCellValue(context.getFieldMeta().getDefaultValue());
        } else if (value instanceof Number) {
            cell.setCellValue(((Number) value).doubleValue());
        } else if (value instanceof Boolean) {
            cell.setCellValue((Boolean) value);
        } else if (value instanceof java.util.Date) {
            cell.setCellValue((java.util.Date) value);
        } else if (value instanceof java.time.LocalDate) {
            cell.setCellValue(java.sql.Date.valueOf((java.time.LocalDate) value));
        } else if (value instanceof java.time.LocalDateTime) {
            cell.setCellValue(java.sql.Timestamp.valueOf((java.time.LocalDateTime) value));
        } else {
            cell.setCellValue(value.toString());
        }

        cell.setCellStyle(context.getColumnStyle());
    }
}
