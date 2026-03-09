package dataset4j.examples;

import dataset4j.annotations.*;

/**
 * Complete example showing compile-time field constant generation and usage.
 * This demonstrates the full workflow from annotation to generated constants to usage.
 */
public class CompileTimeFieldsUsage {
    
    /**
     * After compilation, this record will have the following generated nested classes:
     * 
     * public static final class Fields {
     *     // Field name constants  
     *     public static final String ID = "id";
     *     public static final String FIRST_NAME = "firstName";
     *     public static final String LAST_NAME = "lastName";
     *     public static final String EMAIL = "email";
     *     public static final String DEPARTMENT = "department";
     *     public static final String SALARY = "salary";
     *     public static final String ACTIVE = "active";
     *     
     *     // Utility arrays
     *     public static final String[] ALL_FIELDS = {ID, FIRST_NAME, LAST_NAME, EMAIL, DEPARTMENT, SALARY, ACTIVE};
     *     public static final String[] ANNOTATED_FIELDS = {ID, FIRST_NAME, LAST_NAME, EMAIL, DEPARTMENT, SALARY, ACTIVE};
     * }
     * 
     * public static final class Cols {
     *     // Column name constants (from @DataColumn annotations)
     *     public static final String COL_ID = "Employee ID";
     *     public static final String COL_FIRST_NAME = "First Name";
     *     public static final String COL_LAST_NAME = "Last Name";
     *     public static final String COL_EMAIL = "Email Address";
     *     public static final String COL_DEPARTMENT = "Department";
     *     public static final String COL_SALARY = "Salary";
     *     public static final String COL_ACTIVE = "Active";
     *     
     *     // Column utility arrays
     *     public static final String[] ALL_COLUMNS = {COL_ID, COL_FIRST_NAME, COL_LAST_NAME, COL_EMAIL, COL_DEPARTMENT, COL_SALARY, COL_ACTIVE};
     *     public static final String[] ANNOTATED_COLUMNS = {COL_ID, COL_FIRST_NAME, COL_LAST_NAME, COL_EMAIL, COL_DEPARTMENT, COL_SALARY, COL_ACTIVE};
     * }
     */
    @GenerateFields(
        className = "Fields",
        includeColumnNames = true,
        fieldPrefix = "",
        columnPrefix = "COL_",
        includeIgnored = false
    )
    @DataTable(name = "Employee Report")
    public record Employee(
        @DataColumn(name = "Employee ID", order = 1, required = true, width = 15)
        String id,
        
        @DataColumn(name = "First Name", order = 2, required = true, width = 20)
        String firstName,
        
        @DataColumn(name = "Last Name", order = 3, required = true, width = 20)
        String lastName,
        
        @DataColumn(name = "Email Address", order = 4, required = true, width = 30)
        String email,
        
        @DataColumn(name = "Department", order = 5, width = 25)
        String department,
        
        @DataColumn(name = "Salary", order = 6, cellType = DataColumn.CellType.CURRENCY)
        double salary,
        
        @DataColumn(name = "Active", order = 7)
        boolean active,
        
        @DataColumn(ignore = true)
        String internalNotes
    ) {}
    
    /**
     * Demonstrates how the generated constants would be used in practice.
     * Note: The actual constants will be available after compilation.
     */
    public static void demonstrateUsage() {
        System.out.println("=== Compile-Time Generated Field Constants Usage ===");
        
        // After compilation, you'll be able to use these patterns:
        
        System.out.println("1. Type-safe field selection:");
        System.out.println("   FieldSelector.from(metadata)");
        System.out.println("       .fields(Employee.Fields.ID, Employee.Fields.FIRST_NAME, Employee.Fields.EMAIL)");
        System.out.println("       .select();");
        System.out.println();
        
        System.out.println("2. Column name selection:");
        System.out.println("   FieldSelector.from(metadata)");
        System.out.println("       .columns(Employee.Cols.COL_ID, Employee.Cols.COL_EMAIL)");
        System.out.println("       .select();");
        System.out.println();
        
        System.out.println("3. Bulk selection using arrays:");
        System.out.println("   FieldSelector.from(metadata)");
        System.out.println("       .fieldsArray(Employee.Fields.ALL_FIELDS)  // Select all fields");
        System.out.println("       .select();");
        System.out.println();
        System.out.println("   FieldSelector.from(metadata)");
        System.out.println("       .columnsArray(Employee.Cols.ALL_COLUMNS)  // Select all columns");
        System.out.println("       .select();");
        System.out.println();
        
        System.out.println("4. Excel export with generated constants:");
        System.out.println("   ExcelDatasetWriter");
        System.out.println("       .toFile(\"report.xlsx\")");
        System.out.println("       .fields(Employee.Fields.ID, Employee.Fields.FIRST_NAME, Employee.Fields.SALARY)");
        System.out.println("       .write(employees);");
        System.out.println();
        
        System.out.println("5. Exclusion with constants:");
        System.out.println("   FieldSelector.from(metadata)");
        System.out.println("       .exclude(Employee.Fields.SALARY, Employee.Fields.INTERNAL_NOTES)");
        System.out.println("       .select();");
        System.out.println();
        
        System.out.println("Benefits:");
        System.out.println("  ✓ Separate namespaces for fields and columns");
        System.out.println("  ✓ Clean organization: Fields and Cols classes");
        System.out.println("  ✓ Compile-time safety for both field and column names");
        System.out.println("  ✓ IDE auto-completion and refactoring support");
        System.out.println("  ✓ Better maintainability with clear separation");
        System.out.println("  ✓ Self-documenting code");
        System.out.println("  ✓ Reduced runtime errors");
    }
    
    /**
     * Shows the compilation process and what files are generated.
     */
    public static void showCompilationProcess() {
        System.out.println("=== Compilation Process ===");
        System.out.println();
        System.out.println("1. Write your record with @GenerateFields annotation");
        System.out.println("2. Run 'mvn compile' or build in your IDE");
        System.out.println("3. The annotation processor automatically generates:");
        System.out.println("   - Employee$Fields.class (field name constants)");
        System.out.println("   - Employee$Cols.class (column name constants)");
        System.out.println("   - Utility arrays for bulk operations");
        System.out.println();
        System.out.println("4. Generated constants are available in your code:");
        System.out.println("   - Employee.Fields.ID, Employee.Fields.FIRST_NAME");
        System.out.println("   - Employee.Cols.COL_ID, Employee.Cols.COL_FIRST_NAME");
        System.out.println("   - Employee.Fields.ALL_FIELDS, Employee.Cols.ALL_COLUMNS");
        System.out.println();
        System.out.println("5. Use the constants in your field selection:");
        System.out.println("   - Type-safe");
        System.out.println("   - Auto-complete enabled");
        System.out.println("   - Refactoring friendly");
    }
    
    public static void main(String[] args) {
        demonstrateUsage();
        System.out.println();
        showCompilationProcess();
        
        // Show the metadata that would be available
        System.out.println("\n=== Runtime Metadata ===");
        PojoMetadata<Employee> metadata = MetadataCache.getMetadata(Employee.class);
        System.out.println("Table name: " + metadata.getTableName());
        System.out.println("Field count: " + metadata.getAllFields().size());
        System.out.println("Exportable fields: " + metadata.getExportableFields().size());
        
        System.out.println("\nField details:");
        metadata.getAllFields().forEach(field -> {
            System.out.printf("  %s -> %s (Type: %s, Required: %s)%n",
                field.getFieldName(),
                field.getEffectiveColumnName(), 
                field.getFieldType().getSimpleName(),
                field.isRequired());
        });
    }
}