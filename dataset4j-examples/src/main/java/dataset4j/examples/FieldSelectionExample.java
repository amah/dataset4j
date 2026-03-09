package dataset4j.examples;

import dataset4j.annotations.*;

import java.util.List;

/**
 * Example demonstrating POJO metadata generation and field selection functionality.
 */
public class FieldSelectionExample {
    
    @DataTable(name = "Employee Report")
    public record Employee(
        @DataColumn(name = "ID", order = 1, required = true, width = 15)
        String id,
        
        @DataColumn(name = "First Name", order = 2, required = true, width = 20)
        String firstName,
        
        @DataColumn(name = "Email", order = 4, required = true, width = 30)
        String email,
        
        @DataColumn(name = "Department", order = 5, width = 25)
        String department,
        
        @DataColumn(ignore = true)
        String internalNotes
    ) {}
    
    public static void main(String[] args) {
        demonstrateMetadata();
    }
    
    public static void demonstrateMetadata() {
        System.out.println("=== POJO Metadata Generation ===");
        
        PojoMetadata<Employee> metadata = MetadataCache.getMetadata(Employee.class);
        
        System.out.println("Table: " + metadata.getTableName());
        System.out.println("Total fields: " + metadata.getAllFields().size());
        System.out.println("Exportable fields: " + metadata.getExportableFields().size());
        
        for (FieldMeta field : metadata.getAllFields()) {
            System.out.printf("  %s -> %s (Type: %s, Required: %s)%n",
                    field.getFieldName(),
                    field.getEffectiveColumnName(),
                    field.getFieldType().getSimpleName(),
                    field.isRequired());
        }
        
        System.out.println();
    }
    
    public static List<Employee> createSampleEmployees() {
        return List.of(
                new Employee("E001", "John", "john@company.com", "Engineering", "Notes1"),
                new Employee("E002", "Jane", "jane@company.com", "Marketing", "Notes2")
        );
    }
}