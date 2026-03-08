package dataset4j.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to trigger generation of static field constants for a record class.
 * 
 * When applied to a record class, this annotation instructs the annotation processor
 * to generate a nested Fields class with static constants for each field.
 * 
 * Example usage:
 * {@code
 * @GenerateFields
 * @DataTable(name = "Employee Report")
 * public record Employee(
 *     @DataColumn(name = "ID", order = 1) String id,
 *     @DataColumn(name = "Name", order = 2) String firstName,
 *     @DataColumn(name = "Email", order = 3) String email
 * ) {}
 * }
 * 
 * This will generate:
 * {@code
 * public static final class Fields {
 *     public static final String ID = "id";
 *     public static final String FIRST_NAME = "firstName"; 
 *     public static final String EMAIL = "email";
 * }
 * }
 * 
 * Usage in field selection:
 * {@code
 * FieldSelector.from(metadata)
 *     .fields(Employee.Fields.ID, Employee.Fields.FIRST_NAME, Employee.Fields.EMAIL)
 *     .select();
 * }
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.SOURCE)
public @interface GenerateFields {
    
    /**
     * Name of the generated nested class containing field constants.
     * @return the class name, defaults to "Fields"
     */
    String className() default "Fields";
    
    /**
     * Name of the generated nested class containing column constants.
     * Only used when includeColumnNames is true.
     * @return the class name, defaults to "Cols"
     */
    String columnsClassName() default "Cols";
    
    /**
     * Whether to generate constants for column names as well as field names.
     * @return true to generate both field and column constants
     */
    boolean includeColumnNames() default true;
    
    /**
     * Prefix for field name constants.
     * @return the prefix, empty by default
     */
    String fieldPrefix() default "";
    
    /**
     * Prefix for column name constants.
     * @return the prefix, defaults to "COL_"
     */
    String columnPrefix() default "COL_";
    
    /**
     * Whether to include ignored fields in the generated constants.
     * @return true to include ignored fields, false to exclude them
     */
    boolean includeIgnored() default false;
}