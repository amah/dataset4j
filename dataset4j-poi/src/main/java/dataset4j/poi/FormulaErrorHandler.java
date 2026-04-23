package dataset4j.poi;

import org.apache.poi.ss.usermodel.Cell;

/**
 * Handler invoked when an Excel formula cell cannot be evaluated.
 *
 * <p>The default {@link #BLANK} returns {@code null}, which causes the reader to
 * treat the cell as blank and fall back to the field's default value
 * (hardcoded default, {@code @DataColumn(defaultValue)}, or a user-configured default).
 */
@FunctionalInterface
public interface FormulaErrorHandler {

    /**
     * Called when a formula cell throws during evaluation, or when its cached result is ERROR.
     *
     * @param cell the formula cell
     * @param error the exception thrown by the evaluator, or {@code null} for a cached ERROR value
     * @return substitute value as a String, or {@code null} to treat the cell as blank
     */
    String handle(Cell cell, Exception error);

    /** Default handler: returns {@code null} so the cell is treated as blank. */
    FormulaErrorHandler BLANK = (cell, error) -> null;
}
