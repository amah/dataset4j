# Field Constants Generation

Dataset4j provides compile-time generation of static field constants from annotated record classes, enabling type-safe field selection and eliminating magic strings.

## 🎯 **Overview**

The `@GenerateFields` annotation triggers an annotation processor that generates nested `Fields` classes containing static constants for each field in your records.

### **Before (Magic Strings)**
```java
// Error-prone, no compile-time safety
FieldSelector.from(metadata)
    .fields("id", "firstName", "emial")  // Typo! Runtime error
    .select();
```

### **After (Generated Constants)**
```java
// Type-safe, compile-time checked
FieldSelector.from(metadata)
    .fields(Employee.Fields.ID, Employee.Fields.FIRST_NAME, Employee.Fields.EMAIL)
    .select();
```

## 📝 **Usage**

### **1. Annotate Your Record**

```java
@GenerateFields(
    className = "Fields",           // Name of generated class
    includeColumnNames = true,      // Generate column constants too
    fieldPrefix = "",              // Prefix for field constants  
    columnPrefix = "COL_",         // Prefix for column constants
    includeIgnored = false         // Exclude ignored fields
)
@DataTable(name = "Employee Report")
public record Employee(
    @DataColumn(name = "Employee ID", order = 1, required = true)
    String id,
    
    @DataColumn(name = "First Name", order = 2, required = true) 
    String firstName,
    
    @DataColumn(name = "Email", order = 3, required = true)
    String email,
    
    @DataColumn(ignore = true)
    String internalNotes
) {}
```

### **2. Compile Your Code**

```bash
mvn compile
# or build in your IDE
```

### **3. Generated Constants Available**

The annotation processor automatically generates:

```java
public static final class Fields {
    // Field name constants
    public static final String ID = "id";
    public static final String FIRST_NAME = "firstName"; 
    public static final String EMAIL = "email";
    
    // Column name constants (where different from field names)
    public static final String COL_EMPLOYEE_ID = "Employee ID";
    public static final String COL_FIRST_NAME = "First Name";
    
    // Utility arrays
    public static final String[] ALL_FIELDS = {ID, FIRST_NAME, EMAIL};
    public static final String[] ANNOTATED_FIELDS = {ID, FIRST_NAME, EMAIL};
    public static final String[] ALL_COLUMNS = {"Employee ID", "First Name", "Email"};
}
```

### **4. Use Type-Safe Constants**

```java
// Field selection with constants
List<FieldMeta> fields = FieldSelector.from(metadata)
    .fields(Employee.Fields.ID, Employee.Fields.FIRST_NAME, Employee.Fields.EMAIL)
    .select();

// Excel export with constants  
ExcelDatasetWriter
    .toFile("report.xlsx")
    .fields(Employee.Fields.ID, Employee.Fields.FIRST_NAME, Employee.Fields.EMAIL)
    .write(employees);

// Bulk selection using arrays
FieldSelector.from(metadata)
    .fields(Employee.Fields.ALL_FIELDS)  // All fields at once
    .select();

// Column-based selection
FieldSelector.from(metadata) 
    .columns(Employee.Fields.COL_EMPLOYEE_ID, Employee.Fields.COL_FIRST_NAME)
    .select();

// Exclusion with constants
FieldSelector.from(metadata)
    .exclude(Employee.Fields.INTERNAL_NOTES)
    .exportableOnly()
    .select();
```

## ⚙️ **Configuration Options**

The `@GenerateFields` annotation supports several configuration options:

| Option | Default | Description |
|--------|---------|-------------|
| `className` | `"Fields"` | Name of the generated nested class |
| `includeColumnNames` | `true` | Generate constants for column names |
| `fieldPrefix` | `""` | Prefix for field name constants |
| `columnPrefix` | `"COL_"` | Prefix for column name constants |
| `includeIgnored` | `false` | Include ignored fields in constants |

### **Examples**

**Minimal Configuration:**
```java
@GenerateFields
public record Product(@DataColumn String name, @DataColumn double price) {}
// Generates: Product.Fields.NAME, Product.Fields.PRICE
```

**Custom Prefixes:**
```java
@GenerateFields(fieldPrefix = "FIELD_", columnPrefix = "COLUMN_")
public record Order(@DataColumn(name = "Order ID") String id) {}
// Generates: Order.Fields.FIELD_ID, Order.Fields.COLUMN_ORDER_ID
```

**No Column Constants:**
```java
@GenerateFields(includeColumnNames = false)
public record Customer(@DataColumn String name) {}
// Generates: Customer.Fields.NAME (no column constants)
```

## 🔧 **Integration with Existing APIs**

The generated constants work seamlessly with all field selection APIs:

### **FieldSelector Integration**
```java
// Individual constants
FieldSelector.from(metadata)
    .fields(Employee.Fields.ID, Employee.Fields.NAME)
    .exclude(Employee.Fields.INTERNAL_NOTES)
    .requiredOnly()
    .select();

// Array constants
FieldSelector.from(metadata)
    .fieldsArray(Employee.Fields.ALL_FIELDS)      // String[] support
    .columnsArray(Employee.Fields.ALL_COLUMNS)    // String[] support  
    .select();
```

### **ExcelDatasetWriter Integration**
```java
// Individual constants
ExcelDatasetWriter
    .toFile("report.xlsx")
    .fields(Employee.Fields.ID, Employee.Fields.NAME, Employee.Fields.EMAIL)
    .write(dataset);

// Array constants  
ExcelDatasetWriter
    .toFile("all-fields.xlsx")
    .fieldsArray(Employee.Fields.ALL_FIELDS)      // String[] support
    .write(dataset);

ExcelDatasetWriter
    .toFile("by-columns.xlsx") 
    .columnsArray(Employee.Fields.ALL_COLUMNS)    // String[] support
    .write(dataset);
```

## 🏗️ **Generated Class Structure**

For each `@GenerateFields` annotated record, the processor generates:

### **Field Constants**
- `FIELD_NAME` constants for each record field
- Uses `CONSTANT_CASE` naming convention
- Applies optional prefix from `fieldPrefix`

### **Column Constants** (if `includeColumnNames = true`)
- `COL_COLUMN_NAME` constants for each unique column name
- Only generated when column name differs from field name
- Applies prefix from `columnPrefix`

### **Utility Arrays**
- `ALL_FIELDS` - Array of all field name constants
- `ANNOTATED_FIELDS` - Array of fields with `@DataColumn` annotations
- `ALL_COLUMNS` - Array of all column names (if column generation enabled)

## ✅ **Benefits**

### **Compile-Time Safety**
- **No Magic Strings**: Eliminates runtime errors from typos
- **Type Checking**: Compiler catches field name errors
- **Refactoring Support**: IDE can safely rename fields

### **Developer Experience**
- **Auto-Completion**: IDE suggests available field constants
- **Documentation**: Self-documenting field references
- **Navigation**: Click-through to field definitions

### **Code Maintainability**
- **Single Source of Truth**: Field names defined once in record
- **Consistent Naming**: Generated constants follow conventions
- **Easy Updates**: Annotation changes regenerate constants

### **Performance**
- **Zero Runtime Cost**: Constants resolved at compile time
- **No Reflection**: Direct string constant usage
- **Cached Metadata**: Works with existing caching system

## 🔄 **Build Integration**

### **Maven**
The annotation processor runs automatically during compilation:

```xml
<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-compiler-plugin</artifactId>
            <configuration>
                <annotationProcessorPaths>
                    <path>
                        <groupId>io.github.amah</groupId>
                        <artifactId>dataset4j-core</artifactId>
                        <version>${dataset4j.version}</version>
                    </path>
                </annotationProcessorPaths>
            </configuration>
        </plugin>
    </plugins>
</build>
```

### **IDE Support**
- **IntelliJ IDEA**: Enable annotation processing in settings
- **Eclipse**: Install annotation processing plugin
- **VS Code**: Use Java extension with annotation processor support

## 🎯 **Best Practices**

### **Naming Conventions**
```java
// Good: Clear, descriptive field names
@GenerateFields
public record Employee(
    @DataColumn(name = "Employee ID") String employeeId,    // → EMPLOYEE_ID
    @DataColumn(name = "First Name") String firstName,      // → FIRST_NAME
    @DataColumn(name = "Department") String department       // → DEPARTMENT  
) {}

// Avoid: Unclear or abbreviated names
public record Emp(String id, String nm, String dept) {}    // → ID, NM, DEPT
```

### **Consistent Prefixing**
```java
// Use consistent prefixes across related records
@GenerateFields(fieldPrefix = "EMP_", columnPrefix = "EMP_COL_")
public record Employee(...) {}

@GenerateFields(fieldPrefix = "DEPT_", columnPrefix = "DEPT_COL_")  
public record Department(...) {}
```

### **Logical Grouping**
```java
// Group related fields in arrays for bulk operations
public static final String[] CONTACT_FIELDS = {
    Employee.Fields.EMAIL,
    Employee.Fields.PHONE,
    Employee.Fields.ADDRESS
};

public static final String[] SENSITIVE_FIELDS = {
    Employee.Fields.SALARY,
    Employee.Fields.SSN,
    Employee.Fields.BANK_ACCOUNT
};
```

## 🔍 **Examples**

See the following example classes for complete usage:
- `GeneratedFieldsExample.java` - Basic field constant generation
- `CompileTimeFieldsUsage.java` - Complete workflow demonstration
- `FieldSelectionExample.java` - Runtime metadata with field selection

The field constants generation feature provides a robust, type-safe foundation for selective data export while maintaining excellent developer experience and code maintainability.