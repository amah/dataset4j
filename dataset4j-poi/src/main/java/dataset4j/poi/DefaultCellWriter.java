package dataset4j.poi;

import dataset4j.annotations.DataColumn;
import dataset4j.annotations.FieldMeta;
import org.apache.poi.ss.usermodel.Cell;

import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

/**
 * Default cell-writing logic extracted from the original {@code setCellValue} in
 * {@link ExcelDatasetWriter}. Handles type dispatch (Number, Boolean, Date, etc.)
 * and applies the annotation-driven column style.
 *
 * <p>When {@link DataColumn.WriteAs#STRING} is set on the field, the value is
 * formatted to a string before writing. For date/time types this uses the
 * {@code dateFormat} pattern; for numbers it uses {@code numberFormat} if present,
 * otherwise {@code toString()}.
 */
final class DefaultCellWriter implements CellWriter {

    static final DefaultCellWriter INSTANCE = new DefaultCellWriter();

    private DefaultCellWriter() {}

    @Override
    public void write(CellWriterContext context) {
        Cell cell = context.getCell();
        Object value = context.getValue();
        FieldMeta meta = context.getFieldMeta();

        if (value == null) {
            cell.setCellValue(meta.getDefaultValue());
            cell.setCellStyle(context.getColumnStyle());
            return;
        }

        if (meta.getWriteAs() == DataColumn.WriteAs.STRING) {
            cell.setCellValue(formatAsString(value, meta));
            cell.setCellStyle(context.getColumnStyle());
            return;
        }

        // Native type dispatch (AUTO mode)
        if (value instanceof Number) {
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

    private static String formatAsString(Object value, FieldMeta meta) {
        // Date/time types: use dateFormat pattern
        if (value instanceof TemporalAccessor temporal) {
            String pattern = meta.getDateFormat();
            if (value instanceof java.time.LocalDateTime && pattern.equals("yyyy-MM-dd")) {
                pattern = "yyyy-MM-dd HH:mm:ss";
            }
            return DateTimeFormatter.ofPattern(pattern).format(temporal);
        }

        // Legacy Date
        if (value instanceof java.util.Date date) {
            return new java.text.SimpleDateFormat(meta.getDateFormat()).format(date);
        }

        // Numbers: use numberFormat if present
        if (value instanceof Number number && !meta.getNumberFormat().isEmpty()) {
            return new java.text.DecimalFormat(meta.getNumberFormat()).format(number);
        }

        return value.toString();
    }
}
