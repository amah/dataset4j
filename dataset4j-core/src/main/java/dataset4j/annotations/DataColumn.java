package dataset4j.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Unified data column mapping annotation for structured tabular formats.
 * 
 * Example usage:
 * {@code
 * public record Employee(
 *     @DataColumn(name = "Employee ID", order = 1, required = true,
 *                 cellType = DataColumn.CellType.TEXT, columnIndex = 0)
 *     String id,
 *     
 *     @DataColumn(name = "Salary", order = 6,
 *                 cellType = DataColumn.CellType.CURRENCY,
 *                 numberFormat = "$#,##0.00")
 *     double salary,
 *     
 *     @DataColumn(name = "Join Date", order = 7,
 *                 cellType = DataColumn.CellType.DATE,
 *                 dateFormat = "yyyy-MM-dd")
 *     String joinDate,
 *     
 *     @DataColumn(ignore = true)
 *     String internalNotes
 * ) {}
 * }
 */
@Target(ElementType.RECORD_COMPONENT)
@Retention(RetentionPolicy.RUNTIME)
public @interface DataColumn {
    
    /**
     * Column header name (defaults to field name).
     * @return the column name, or empty string for default field name
     */
    String name() default "";
    
    /**
     * Column order/position for logical ordering. Lower numbers appear first.
     * Use -1 for default ordering (field declaration order).
     * @return the order value, or -1 for default ordering
     */
    int order() default -1;
    
    /**
     * Specific column index (A=0, B=1, etc.). Use -1 for auto-assignment.
     * @return the column index, or -1 for automatic positioning
     */
    int columnIndex() default -1;
    
    /**
     * Whether this column is required (non-null/non-empty).
     * @return true if field is required, false otherwise
     */
    boolean required() default false;
    
    /**
     * Whether to ignore this field during export/import.
     * @return true to ignore field, false to include
     */
    boolean ignore() default false;
    
    /**
     * Optional description for documentation.
     * @return the description text
     */
    String description() default "";
    
    /**
     * Default value to use if field is null/empty during export.
     * @return the default value as string
     */
    String defaultValue() default "";
    
    /**
     * Whether the column should be hidden by default.
     * @return true to hide column, false to show
     */
    boolean hidden() default false;
    
    /**
     * Cell type for proper data formatting.
     * @return the cell type for formatting
     */
    CellType cellType() default CellType.AUTO;
    
    /**
     * Number format pattern (standard format string).
     * Examples: "#,##0.00", "$#,##0.00", "0.00%"
     * @return the number format pattern
     */
    String numberFormat() default "";
    
    /**
     * Date format pattern for date fields.
     * Examples: "yyyy-MM-dd", "MM/dd/yyyy", "dd-MMM-yyyy"
     * @return the date format pattern
     */
    String dateFormat() default "yyyy-MM-dd";
    
    /**
     * Background color (hex format: #RRGGBB or standard color name).
     * @return the background color
     */
    String backgroundColor() default "";
    
    /**
     * Font color (hex format: #RRGGBB or standard color name).
     * @return the font color
     */
    String fontColor() default "";
    
    /**
     * Whether the column should be bold.
     * @return true for bold text, false otherwise
     */
    boolean bold() default false;
    
    /**
     * Whether the column should be frozen (freeze panes).
     * @return true to freeze column, false otherwise
     */
    boolean frozen() default false;
    
    /**
     * Column width in characters. Use -1 for auto-width.
     * @return the column width, or -1 for automatic width
     */
    int width() default -1;
    
    /**
     * Whether to wrap text in the cell.
     * @return true to wrap text, false otherwise
     */
    boolean wrapText() default false;
    
    /**
     * Text alignment in the cell.
     * @return the text alignment option
     */
    Alignment alignment() default Alignment.AUTO;
    
    /**
     * Cell types for proper data handling.
     */
    enum CellType {
        /** Infer from field type */
        AUTO,
        /** String values */
        TEXT,
        /** Numeric values */
        NUMBER,
        /** Date/time values */
        DATE,
        /** Boolean values */
        BOOLEAN,
        /** Currency formatting */
        CURRENCY,
        /** Percentage formatting */
        PERCENTAGE,
        /** Formula/calculated field */
        FORMULA
    }
    
    /**
     * Text alignment options.
     */
    enum Alignment {
        /** Default alignment based on data type */
        AUTO,
        /** Left-aligned */
        LEFT,
        /** Center-aligned */
        CENTER,
        /** Right-aligned */
        RIGHT
    }
}