package dataset4j.poi;

import dataset4j.Dataset;
import dataset4j.annotations.FieldMeta;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.IndexedColors;

import java.lang.reflect.RecordComponent;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Package-private helper that renders a single {@link Dataset} as one sheet
 * inside an existing {@link Workbook}. Shared by {@link ExcelDatasetWriter}
 * (single-sheet) and {@link ExcelWorkbookWriter} (multi-sheet).
 */
final class ExcelSheetRenderer {

    private ExcelSheetRenderer() {}

    /** Appends a new sheet to {@code workbook} and writes {@code dataset} into it. */
    static <T> void renderSheet(
            Workbook workbook,
            String sheetName,
            Dataset<T> dataset,
            SheetRenderConfig config) {

        Sheet sheet = workbook.createSheet(sheetName);

        if (dataset.isEmpty()) {
            return; // empty sheet is fine, nothing more to write
        }

        CellStyle headerStyle = createHeaderStyle(workbook);
        Map<String, CellStyle> columnStyles = buildColumnStyles(config.fieldsToExport(), workbook);

        int rowIndex = 0;

        if (config.includeHeaders()) {
            Row headerRow = sheet.createRow(rowIndex++);
            writeHeaders(headerRow, config.fieldsToExport(), headerStyle);
        }

        for (T record : dataset.toList()) {
            Row dataRow = sheet.createRow(rowIndex++);
            writeDataRow(dataRow, record, config, columnStyles, workbook);
        }

        applyColumnFormatting(sheet, config.fieldsToExport(), config.includeHeaders());

        if (config.autoSizeColumns()) {
            for (int i = 0; i < config.fieldsToExport().size(); i++) {
                sheet.autoSizeColumn(i);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private static void writeHeaders(Row headerRow, List<FieldMeta> fields, CellStyle headerStyle) {
        for (int i = 0; i < fields.size(); i++) {
            Cell cell = headerRow.createCell(i);
            cell.setCellValue(fields.get(i).getEffectiveColumnName());
            cell.setCellStyle(headerStyle);
        }
    }

    private static <T> void writeDataRow(
            Row row,
            T record,
            SheetRenderConfig config,
            Map<String, CellStyle> columnStyles,
            Workbook workbook) {

        RecordComponent[] components = record.getClass().getRecordComponents();

        for (int i = 0; i < config.fieldsToExport().size(); i++) {
            FieldMeta fieldMeta = config.fieldsToExport().get(i);
            Cell cell = row.createCell(i);

            RecordComponent component = findComponent(components, fieldMeta.getFieldName());
            if (component != null) {
                try {
                    Object value = component.getAccessor().invoke(record);
                    if (value == null) {
                        value = resolveWriteDefault(fieldMeta, config);
                    }
                    CellWriter writer = resolveWriter(fieldMeta.getFieldName(), config);
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

    private static Object resolveWriteDefault(FieldMeta fieldMeta, SheetRenderConfig config) {
        if (config.fieldDefaults().containsKey(fieldMeta.getFieldName())) {
            return config.fieldDefaults().get(fieldMeta.getFieldName());
        }
        if (config.typeDefaults().containsKey(fieldMeta.getFieldType())) {
            return config.typeDefaults().get(fieldMeta.getFieldType());
        }
        return null;
    }

    private static CellWriter resolveWriter(String fieldName, SheetRenderConfig config) {
        CellWriter perField = config.fieldCellWriters().get(fieldName);
        if (perField != null) return perField;
        if (config.globalCellWriter() != null) return config.globalCellWriter();
        return DefaultCellWriter.INSTANCE;
    }

    private static RecordComponent findComponent(RecordComponent[] components, String fieldName) {
        for (RecordComponent component : components) {
            if (component.getName().equals(fieldName)) {
                return component;
            }
        }
        return null;
    }

    private static Map<String, CellStyle> buildColumnStyles(List<FieldMeta> fields, Workbook workbook) {
        DataFormat dataFormat = workbook.createDataFormat();
        Map<String, CellStyle> styles = new HashMap<>();

        for (FieldMeta fieldMeta : fields) {
            CellStyle style = workbook.createCellStyle();

            if (!fieldMeta.getNumberFormat().isEmpty()) {
                style.setDataFormat(dataFormat.getFormat(fieldMeta.getNumberFormat()));
            }

            Class<?> fieldType = fieldMeta.getFieldType();
            if (fieldType == java.time.LocalDate.class || fieldType == java.time.LocalDateTime.class) {
                String dateFormat = fieldMeta.getDateFormat();
                if (fieldType == java.time.LocalDateTime.class && dateFormat.equals("yyyy-MM-dd")) {
                    dateFormat = "yyyy-MM-dd HH:mm:ss";
                }
                style.setDataFormat(dataFormat.getFormat(JavaToExcelDateFormat.convert(dateFormat)));
            }

            if (fieldMeta.isBold() || !fieldMeta.getFontColor().isEmpty()) {
                Font font = workbook.createFont();
                if (fieldMeta.isBold()) font.setBold(true);
                if (!fieldMeta.getFontColor().isEmpty()) {
                    try {
                        font.setColor(getColorIndex(fieldMeta.getFontColor()));
                    } catch (Exception ignored) {}
                }
                style.setFont(font);
            }

            if (!fieldMeta.getBackgroundColor().isEmpty()) {
                try {
                    style.setFillForegroundColor(getColorIndex(fieldMeta.getBackgroundColor()));
                    style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
                } catch (Exception ignored) {}
            }

            if (fieldMeta.isWrapText()) style.setWrapText(true);

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

    private static void applyColumnFormatting(Sheet sheet, List<FieldMeta> fields, boolean hasHeaders) {
        int freezeColumn = -1;

        for (int i = 0; i < fields.size(); i++) {
            FieldMeta field = fields.get(i);
            if (field.getWidth() > 0) {
                sheet.setColumnWidth(i, field.getWidth() * 256);
            }
            if (field.isFrozen() && freezeColumn < i) {
                freezeColumn = i;
            }
        }

        if (freezeColumn >= 0) {
            sheet.createFreezePane(freezeColumn + 1, hasHeaders ? 1 : 0);
        }
    }

    static CellStyle createHeaderStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        Font font = workbook.createFont();
        font.setBold(true);
        style.setFont(font);
        style.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setBorderTop(BorderStyle.THIN);
        style.setBorderBottom(BorderStyle.THIN);
        style.setBorderLeft(BorderStyle.THIN);
        style.setBorderRight(BorderStyle.THIN);
        return style;
    }

    private static short getColorIndex(String color) {
        if (color.startsWith("#")) {
            return IndexedColors.AUTOMATIC.getIndex();
        }
        return switch (color.toLowerCase()) {
            case "red"              -> IndexedColors.RED.getIndex();
            case "blue"             -> IndexedColors.BLUE.getIndex();
            case "green"            -> IndexedColors.GREEN.getIndex();
            case "yellow"           -> IndexedColors.YELLOW.getIndex();
            case "orange"           -> IndexedColors.ORANGE.getIndex();
            case "gray", "grey"     -> IndexedColors.GREY_25_PERCENT.getIndex();
            case "black"            -> IndexedColors.BLACK.getIndex();
            case "white"            -> IndexedColors.WHITE.getIndex();
            default                 -> IndexedColors.AUTOMATIC.getIndex();
        };
    }
}
