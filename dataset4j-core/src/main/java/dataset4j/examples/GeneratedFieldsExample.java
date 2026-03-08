package dataset4j.examples;

import dataset4j.annotations.*;

import java.util.List;

/**
 * Example demonstrating automatic generation of field constants from annotated records.
 * This shows how to use @GenerateFields annotation for compile-time type safety.
 */
public class GeneratedFieldsExample {
    
    /**
     * Example record with @GenerateFields annotation.
     * This will automatically generate a nested Fields class with static constants.
     */
    @GenerateFields(
        className = "Fields",           // Name of generated nested class
        includeColumnNames = true,      // Generate constants for column names too
        fieldPrefix = "",              // No prefix for field constants
        columnPrefix = "COL_",         // Prefix for column constants
        includeIgnored = false         // Don't generate constants for ignored fields
    )
    @DataTable(name = "Employee Report", description = "Employee data with generated field constants")
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
        
        @DataColumn(name = "Salary", order = 6, 
                   cellType = DataColumn.CellType.CURRENCY,
                   numberFormat = "$#,##0.00")
        double salary,
        
        @DataColumn(name = "Active", order = 7)
        boolean active,
        
        // This field will be ignored in constant generation
        @DataColumn(ignore = true)
        String internalNotes
    ) {
        // The @GenerateFields annotation will generate a nested class like this:
        //
        // public static final class Fields {
        //     // Field name constants
        //     public static final String ID = "id";
        //     public static final String FIRST_NAME = "firstName";
        //     public static final String LAST_NAME = "lastName";
        //     public static final String EMAIL = "email";
        //     public static final String DEPARTMENT = "department";
        //     public static final String SALARY = "salary";
        //     public static final String ACTIVE = "active";
        //     
        //     // Column name constants (where different from field names)
        //     public static final String COL_EMPLOYEE_ID = "Employee ID";
        //     public static final String COL_FIRST_NAME = "First Name";
        //     public static final String COL_LAST_NAME = "Last Name";
        //     public static final String COL_EMAIL_ADDRESS = "Email Address";
        //     
        //     // Utility arrays
        //     public static final String[] ALL_FIELDS = {ID, FIRST_NAME, LAST_NAME, EMAIL, DEPARTMENT, SALARY, ACTIVE};
        //     public static final String[] ANNOTATED_FIELDS = {ID, FIRST_NAME, LAST_NAME, EMAIL, DEPARTMENT, SALARY, ACTIVE};
        //     public static final String[] ALL_COLUMNS = {"Employee ID", "First Name", "Last Name", "Email Address", "Department", "Salary", "Active"};
        // }
    }
    
    /**
     * Department record with minimal field generation.
     */
    @GenerateFields(includeColumnNames = false)
    @DataTable(name = "Department")
    public record Department(
        @DataColumn(name = "Code", required = true)
        String code,
        
        @DataColumn(name = "Name", required = true)
        String name,
        
        @DataColumn(name = "Manager")
        String manager
    ) {}
    
    public static void main(String[] args) {
        demonstrateGeneratedConstants();
        demonstrateTypeSafeFieldSelection();
        demonstrateUtilityArrays();
    }
    
    /**
     * Demonstrate the generated field constants.
     * Note: This example shows the intended usage - the actual constants
     * will be generated at compile time by the annotation processor.
     */
    public static void demonstrateGeneratedConstants() {
        System.out.println("=== Generated Field Constants ===");
        
        // In actual usage after compilation, you would access:
        System.out.println("Field constants (will be generated at compile time):");
        System.out.println("  Employee.Fields.ID");
        System.out.println("  Employee.Fields.FIRST_NAME");
        System.out.println("  Employee.Fields.LAST_NAME");
        System.out.println("  Employee.Fields.EMAIL");
        System.out.println("  Employee.Fields.DEPARTMENT");
        System.out.println("  Employee.Fields.SALARY");
        System.out.println("  Employee.Fields.ACTIVE");
        
        System.out.println("\nColumn constants:");
        System.out.println("  Employee.Fields.COL_EMPLOYEE_ID = \"Employee ID\"");
        System.out.println("  Employee.Fields.COL_FIRST_NAME = \"First Name\"");
        System.out.println("  Employee.Fields.COL_LAST_NAME = \"Last Name\"");
        System.out.println("  Employee.Fields.COL_EMAIL_ADDRESS = \"Email Address\"");
        
        System.out.println();
    }
    
    /**
     * Demonstrate type-safe field selection using generated constants.
     * This replaces magic strings with compile-time checked constants.
     */
    public static void demonstrateTypeSafeFieldSelection() {
        System.out.println("=== Type-Safe Field Selection ===");
        
        PojoMetadata<Employee> metadata = MetadataCache.getMetadata(Employee.class);
        
        // Before (magic strings - error-prone):
        System.out.println("Old approach (magic strings):");
        System.out.println("  .fields(\"id\", \"firstName\", \"email\")  // Typo-prone!");
        
        // After (type-safe constants - will be available after compilation):
        System.out.println("\nNew approach (type-safe constants):");
        System.out.println("  .fields(Employee.Fields.ID, Employee.Fields.FIRST_NAME, Employee.Fields.EMAIL)");
        
        // Example of how the generated constants would be used:
        /* 
        List<FieldMeta> basicFields = FieldSelector.from(metadata)
            .fields(Employee.Fields.ID, Employee.Fields.FIRST_NAME, Employee.Fields.EMAIL)
            .select();
        
        List<FieldMeta> contactFields = FieldSelector.from(metadata)
            .columns(Employee.Fields.COL_EMPLOYEE_ID, Employee.Fields.COL_EMAIL_ADDRESS)
            .select();
            
        List<FieldMeta> excludeSensitive = FieldSelector.from(metadata)
            .exclude(Employee.Fields.SALARY, Employee.Fields.INTERNAL_NOTES)
            .select();
        */
        
        System.out.println("\nBenefits:");
        System.out.println("  ✓ Compile-time type safety");
        System.out.println("  ✓ IDE auto-completion");
        System.out.println("  ✓ Refactoring support");
        System.out.println("  ✓ No magic strings");
        System.out.println("  ✓ Better code maintainability");
        
        System.out.println();
    }
    
    /**
     * Demonstrate utility arrays generated for common use cases.
     */
    public static void demonstrateUtilityArrays() {
        System.out.println("=== Generated Utility Arrays ===");
        
        System.out.println("The annotation processor also generates utility arrays:");
        System.out.println("\n1. ALL_FIELDS - All field names:");
        System.out.println("   Employee.Fields.ALL_FIELDS");
        System.out.println("   // [\"id\", \"firstName\", \"lastName\", \"email\", \"department\", \"salary\", \"active\"]");
        
        System.out.println("\n2. ANNOTATED_FIELDS - Fields with @DataColumn:");
        System.out.println("   Employee.Fields.ANNOTATED_FIELDS");
        System.out.println("   // [\"id\", \"firstName\", \"lastName\", \"email\", \"department\", \"salary\", \"active\"]");
        
        System.out.println("\n3. ALL_COLUMNS - Column names:");
        System.out.println("   Employee.Fields.ALL_COLUMNS");
        System.out.println("   // [\"Employee ID\", \"First Name\", \"Last Name\", \"Email Address\", \"Department\", \"Salary\", \"Active\"]");
        
        System.out.println("\nUsage in field selection:");
        System.out.println("  // Select all fields at once");
        System.out.println("  .fields(Employee.Fields.ALL_FIELDS)");
        System.out.println("  ");
        System.out.println("  // Select by column names");
        System.out.println("  .columns(Employee.Fields.ALL_COLUMNS)");
        
        System.out.println();
    }
    
    /**
     * Create sample data for testing.
     */
    public static List<Employee> createSampleEmployees() {
        return List.of(
            new Employee("E001", "John", "Doe", "john.doe@company.com", 
                        "Engineering", 75000.0, true, "Top performer"),
            new Employee("E002", "Jane", "Smith", "jane.smith@company.com",
                        "Marketing", 65000.0, true, "Creative lead"),
            new Employee("E003", "Bob", "Johnson", "bob.johnson@company.com",
                        "Engineering", 80000.0, false, "On leave")
        );
    }
    
    /**
     * Create sample departments for testing.
     */
    public static List<Department> createSampleDepartments() {
        return List.of(
            new Department("ENG", "Engineering", "Alice Johnson"),
            new Department("MKT", "Marketing", "Bob Williams"),
            new Department("HR", "Human Resources", "Carol Brown")
        );
    }
}