package dataset4j.poi;

import dataset4j.annotations.FieldMeta;

import java.util.List;
import java.util.Map;

/**
 * Holds per-sheet rendering configuration for {@link ExcelSheetRenderer}.
 */
record SheetRenderConfig(
        List<FieldMeta> fieldsToExport,
        boolean includeHeaders,
        boolean autoSizeColumns,
        CellWriter globalCellWriter,
        Map<String, CellWriter> fieldCellWriters,
        Map<Class<?>, Object> typeDefaults,
        Map<String, Object> fieldDefaults) {}
