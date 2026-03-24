package dataset4j.poi;

/**
 * Functional interface for customizing how values are written to Excel cells.
 *
 * <p>Register globally or per-field on {@link ExcelDatasetWriter}:
 * {@code
 * ExcelDatasetWriter.toFile("out.xlsx")
 *     .cellWriter(ctx -> {
 *         if (ctx.getValue() instanceof Number n && n.doubleValue() < 0) {
 *             // custom styling for negatives
 *         } else {
 *             ctx.writeDefault();
 *         }
 *     })
 *     .write(dataset);
 * }
 */
@FunctionalInterface
public interface CellWriter {
    void write(CellWriterContext context);
}
