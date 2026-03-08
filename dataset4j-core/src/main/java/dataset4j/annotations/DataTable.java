package dataset4j.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for specifying table-level metadata for record classes.
 * 
 * Example usage:
 * {@code
 * @DataTable(
 *     name = "Employee Report",
 *     description = "Monthly employee data export",
 *     headers = true
 * )
 * public record Employee(
 *     @DataColumn(name = "ID", order = 1) String id,
 *     @DataColumn(name = "Name", order = 2) String name
 * ) {}
 * }
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DataTable {
    
    /**
     * Table/report name.
     * @return the table name
     */
    String name() default "";
    
    /**
     * Table description.
     * @return the table description
     */
    String description() default "";
    
    /**
     * Whether to include column headers.
     * @return true to include headers, false otherwise
     */
    boolean headers() default true;
    
    /**
     * Default date format for the table.
     * @return the default date format pattern
     */
    String defaultDateFormat() default "yyyy-MM-dd";
    
    /**
     * Default number format for the table.
     * @return the default number format pattern
     */
    String defaultNumberFormat() default "";
    
    /**
     * Whether to validate data during import.
     * @return true to validate on import, false otherwise
     */
    boolean validateOnImport() default false;
    
    /**
     * Whether to skip empty rows during import.
     * @return true to skip empty rows, false otherwise
     */
    boolean skipEmptyRows() default true;
    
    /**
     * Maximum number of rows to process (-1 for unlimited).
     * @return the maximum number of rows to process
     */
    int maxRows() default -1;
    
    /**
     * Custom styling options.
     * @return array of CSS class names for styling
     */
    String[] styleClasses() default {};
}