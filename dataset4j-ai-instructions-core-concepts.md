# Dataset4J - Core Concepts for AI Assistants

## 1. Dataset<T> - Immutable DataFrame

The core of Dataset4J is the `Dataset<T>` class, which provides a pandas-like API while maintaining Java's type safety and immutability.

```java
// Core immutable DataFrame class
public final class Dataset<T> {
    private final List<T> rows;  // Immutable record collection
    
    // Fluent API methods return new Dataset instances
    public <R> Dataset<R> map(Function<T, R> mapper);
    public Dataset<T> filter(Predicate<T> predicate);
    public <K> Dataset<T> groupBy(Function<T, K> keyExtractor);
    public Dataset<T> sorted(Comparator<T> comparator);
    public Dataset<T> limit(int count);
    // ... more operations
}
```

### Key Principles

- **Immutable**: All operations return new Dataset instances
- **Type-safe**: Leverages Java records and generics
- **Fluent API**: Chainable operations like pandas
- **Lazy evaluation**: Where beneficial for performance

### Basic Operations

```java
// Creating datasets
Dataset<Employee> employees = Dataset.of(
    new Employee("001", "John", "john@example.com"),
    new Employee("002", "Jane", "jane@example.com")
);

// Chaining operations (immutable)
Dataset<String> result = employees
    .filter(emp -> emp.name().startsWith("J"))  // Returns new Dataset
    .map(Employee::email)                       // Returns new Dataset
    .sorted(String::compareTo);                 // Returns new Dataset
```

## 2. Record-Based Data Model

Dataset4J is built around Java records with rich annotation support for metadata.

```java
@GenerateFields(className = "Fields", columnsClassName = "Cols")
@DataTable(name = "Employee Report") 
public record Employee(
    @DataColumn(name = "Employee ID", order = 1, required = true, width = 15)
    String id,
    
    @DataColumn(name = "First Name", order = 2, required = true, width = 20)
    String firstName,
    
    @DataColumn(name = "Email", order = 3, cellType = DataColumn.CellType.TEXT, width = 30)
    String email,
    
    @DataColumn(name = "Salary", order = 4, cellType = DataColumn.CellType.CURRENCY, numberFormat = "#,##0.00")
    double salary,
    
    @DataColumn(ignore = true)  // Excluded from exports
    String internalNotes
) {}
```

### Annotation Reference

#### @GenerateFields
Triggers compile-time generation of static field constants:

```java
@GenerateFields(
    className = "Fields",        // Generated class name for field constants
    columnsClassName = "Cols",   // Generated class name for column constants
    fieldPrefix = "",            // Prefix for field constants (default: "")
    columnPrefix = "COL_",       // Prefix for column constants (default: "COL_")
    includeIgnored = false       // Include @DataColumn(ignore=true) fields
)
```

#### @DataTable
Table-level metadata:

```java
@DataTable(
    name = "Display Name"        // Human-readable table name
)
```

#### @DataColumn
Field-level metadata:

```java
@DataColumn(
    name = "Column Display Name",           // Column header text
    order = 1,                             // Sort/display order
    required = true,                       // Validation flag
    width = 20,                           // Excel column width
    cellType = DataColumn.CellType.TEXT,   // Cell formatting hint
    numberFormat = "#,##0.00",            // Number formatting pattern
    dateFormat = "yyyy-MM-dd",            // Date formatting pattern
    ignore = false                        // Exclude from exports
)
```

## 3. Compile-Time Field Constants Generation

The `@GenerateFields` annotation triggers automatic generation of type-safe field constants at compile time.

### Generated Structure

For an annotated record, the annotation processor generates:

```java
// Generated at compile time - Employee.Fields
public static final class Fields {
    private Fields() { /* Utility class */ }
    
    // Field name constants (Java field names)
    public static final String ID = "id";
    public static final String FIRST_NAME = "firstName";
    public static final String EMAIL = "email";
    public static final String SALARY = "salary";
    
    // Utility arrays
    public static final String[] ALL_FIELDS = {ID, FIRST_NAME, EMAIL, SALARY};
    public static final String[] ANNOTATED_FIELDS = {ID, FIRST_NAME, EMAIL, SALARY};
}

// Generated at compile time - Employee.Cols  
public static final class Cols {
    private Cols() { /* Utility class */ }
    
    // Column name constants (from @DataColumn annotations)
    public static final String COL_ID = "Employee ID";
    public static final String COL_FIRST_NAME = "First Name";  
    public static final String COL_EMAIL = "Email";
    public static final String COL_SALARY = "Salary";
    
    // Utility arrays
    public static final String[] ALL_COLUMNS = {COL_ID, COL_FIRST_NAME, COL_EMAIL, COL_SALARY};
    public static final String[] ANNOTATED_COLUMNS = {COL_ID, COL_FIRST_NAME, COL_EMAIL, COL_SALARY};
}
```

### Benefits of Generated Constants

- ✅ **Compile-time safety**: No typos in field names
- ✅ **IDE support**: Auto-completion and refactoring
- ✅ **Self-documenting**: Constants show available fields
- ✅ **Bulk operations**: Arrays for selecting all fields
- ✅ **Separation**: Fields vs Columns for different use cases

## 4. Runtime Metadata System

The metadata system provides runtime access to field information with caching for performance.

```java
// Get cached metadata for a record class
PojoMetadata<Employee> metadata = MetadataCache.getMetadata(Employee.class);

// Access field information
List<FieldMeta> allFields = metadata.getAllFields();
List<FieldMeta> exportableFields = metadata.getExportableFields();
List<FieldMeta> requiredFields = metadata.getRequiredFields();

// Field metadata details
for (FieldMeta field : allFields) {
    String fieldName = field.getFieldName();           // "firstName"
    String columnName = field.getEffectiveColumnName(); // "First Name"  
    Class<?> fieldType = field.getFieldType();          // String.class
    boolean required = field.isRequired();              // true/false
    int order = field.getOrder();                       // 1, 2, 3...
    boolean ignored = field.isIgnored();                // true/false
}
```

### Metadata Caching

```java
// Thread-safe caching system
public class MetadataCache {
    private static final ConcurrentHashMap<Class<?>, PojoMetadata<?>> cache = new ConcurrentHashMap<>();
    
    public static <T> PojoMetadata<T> getMetadata(Class<T> recordClass) {
        return (PojoMetadata<T>) cache.computeIfAbsent(recordClass, PojoMetadata::of);
    }
}
```

**Caching Benefits:**
- ✅ **Performance**: Reflection only happens once per class
- ✅ **Thread-safe**: Concurrent access without synchronization overhead  
- ✅ **Memory efficient**: Metadata shared across instances

## 5. Field Selection API

The `FieldSelector` provides a fluent API for selecting and filtering fields from metadata.

```java
// Basic field selection
List<FieldMeta> fields = FieldSelector.from(metadata)
    .fields(Employee.Fields.ID, Employee.Fields.FIRST_NAME, Employee.Fields.EMAIL)
    .exclude(Employee.Fields.INTERNAL_NOTES)
    .requiredOnly()
    .orderByAnnotation()
    .select();

// Column-based selection  
List<FieldMeta> columnFields = FieldSelector.from(metadata)
    .columns(Employee.Cols.COL_ID, Employee.Cols.COL_FIRST_NAME)
    .select();

// Bulk operations with generated arrays
List<FieldMeta> allFields = FieldSelector.from(metadata)
    .fieldsArray(Employee.Fields.ALL_FIELDS)  // Uses generated array
    .select();

// Conditional selection
List<FieldMeta> filteredFields = FieldSelector.from(metadata)
    .where(field -> field.getWidth() > 0)
    .ofType(String.class)
    .withFormatting()
    .select();
```

### Selection Methods

| Method | Description | Example |
|--------|-------------|---------|
| `.fields(...)` | Select specific fields by name | `.fields(Employee.Fields.ID, Employee.Fields.NAME)` |
| `.columns(...)` | Select by column names | `.columns(Employee.Cols.COL_ID, Employee.Cols.COL_NAME)` |
| `.fieldsArray(...)` | Bulk field selection | `.fieldsArray(Employee.Fields.ALL_FIELDS)` |
| `.columnsArray(...)` | Bulk column selection | `.columnsArray(Employee.Cols.ALL_COLUMNS)` |
| `.exclude(...)` | Exclude fields | `.exclude(Employee.Fields.INTERNAL_NOTES)` |
| `.requiredOnly()` | Only required fields | Filters by `@DataColumn(required=true)` |
| `.exportableOnly()` | Only non-ignored fields | Excludes `@DataColumn(ignore=true)` |
| `.ofType(Class)` | Fields of specific type | `.ofType(String.class)` |
| `.withFormatting()` | Fields with format rules | Has number/date formatting |
| `.where(Predicate)` | Custom conditions | `.where(field -> field.getWidth() > 0)` |

## 6. Type Safety Guidelines

### Generic Type Preservation

```java
// ✅ Good: Maintain type information throughout
public <T> Dataset<T> processRecords(Dataset<T> input, Class<T> recordClass) {
    PojoMetadata<T> metadata = MetadataCache.getMetadata(recordClass);
    return input.filter(createValidationFilter(metadata));
}

// ✅ Good: Use bounded type parameters when appropriate
public <T extends Record> void exportToExcel(Dataset<T> data, String filename) {
    ExcelDatasetWriter.toFile(filename).write(data);
}
```

### Immutability Patterns

```java
// ✅ Good: Chain operations for efficiency
Dataset<Employee> processed = employees
    .filter(emp -> emp.salary() > 50000)     // Returns new Dataset
    .sorted(Comparator.comparing(Employee::firstName))  // Returns new Dataset
    .limit(100);                             // Returns new Dataset

// ❌ Avoid: Converting to mutable collections
List<Employee> list = employees.toList();   // Don't do this
list.removeIf(emp -> emp.salary() <= 50000); // Breaks immutability
```

### Record Creation Patterns

```java
// ✅ Good: Define record types for transformations
public record EmployeeSummary(String name, String department, double salary) {}

Dataset<EmployeeSummary> summaries = employees
    .map(emp -> new EmployeeSummary(emp.firstName(), emp.department(), emp.salary()));

// ✅ Good: Use generated constants consistently
List<FieldMeta> selectedFields = FieldSelector.from(metadata)
    .fields(Employee.Fields.NAME, Employee.Fields.DEPARTMENT, Employee.Fields.SALARY)
    .select();
```

This covers the core concepts that AI assistants need to understand when working with Dataset4J. The emphasis is on type safety, immutability, and leveraging the annotation-driven metadata system.