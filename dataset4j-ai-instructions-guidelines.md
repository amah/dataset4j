# Dataset4J - AI Assistant Guidelines for Code Generation

This guide provides specific guidelines for AI assistants when generating Dataset4J code, focusing on best practices, error handling, and optimization.

## Code Generation Best Practices

### Always Use These Patterns

#### 1. Java Records for Data Structures

```java
// ✅ Always define record types for data
@GenerateFields(className = "Fields", columnsClassName = "Cols")
@DataTable(name = "Employee Report")
public record Employee(
    @DataColumn(name = "Employee ID", order = 1, required = true)
    String id,
    @DataColumn(name = "Full Name", order = 2, required = true) 
    String fullName,
    @DataColumn(name = "Email", order = 3)
    String email
) {}

// ❌ Never use Map or dynamic structures
Map<String, Object> employee = new HashMap<>();  // Don't do this
employee.put("id", "001");
employee.put("name", "John");
```

#### 2. Generated Constants Instead of Magic Strings

```java
// ✅ Use generated field constants
List<FieldMeta> fields = FieldSelector.from(metadata)
    .fields(Employee.Fields.ID, Employee.Fields.FULL_NAME, Employee.Fields.EMAIL)
    .select();

ExcelDatasetWriter
    .toFile("report.xlsx")
    .fields(Employee.Fields.ID, Employee.Fields.FULL_NAME)
    .write(dataset);

// ❌ Never use magic strings
List<FieldMeta> fields = FieldSelector.from(metadata)
    .fields("id", "fullName", "email")  // Don't do this - typos cause runtime errors
    .select();
```

#### 3. Immutable Dataset Operations

```java
// ✅ Chain operations fluently (returns new instances)
Dataset<Employee> result = employees
    .filter(emp -> emp.salary() > 50000)     // Returns new Dataset
    .sorted(Comparator.comparing(Employee::fullName))  // Returns new Dataset
    .limit(100);                             // Returns new Dataset

// ❌ Never mutate original data
List<Employee> list = employees.toList();   // Don't do this
list.removeIf(emp -> emp.salary() <= 50000); // Breaks immutability
```

#### 4. Annotation-Driven Configuration

```java
// ✅ Use comprehensive annotations
@GenerateFields(
    className = "Fields",
    columnsClassName = "Cols", 
    fieldPrefix = "",
    columnPrefix = "COL_",
    includeIgnored = false
)
@DataTable(name = "Employee Database")
public record Employee(
    @DataColumn(name = "ID", order = 1, required = true, width = 10)
    String id,
    @DataColumn(name = "Full Name", order = 2, required = true, width = 25)
    String fullName,
    @DataColumn(ignore = true)  // Excluded from exports
    String internalNotes
) {}

// ❌ Don't rely on defaults when configuration is needed
public record Employee(String id, String name) {}  // No metadata for export
```

## Type Safety Guidelines

### Generic Type Preservation

```java
// ✅ Maintain type information throughout operations
public <T> Dataset<T> processValidRecords(Dataset<T> input, Class<T> recordClass) {
    PojoMetadata<T> metadata = MetadataCache.getMetadata(recordClass);
    return input.filter(record -> isValid(record, metadata));
}

// ✅ Use bounded type parameters when appropriate
public <T extends Record> void exportToExcel(Dataset<T> data, String filename) {
    PojoMetadata<T> metadata = MetadataCache.getMetadata((Class<T>) data.getRecordClass());
    ExcelDatasetWriter.toFile(filename)
        .fieldsArray(getAllFieldConstants(metadata))
        .write(data);
}

// ❌ Don't lose type information
public Dataset processRecords(Dataset input) {  // Raw types lose safety
    return input.filter(record -> true);  // No compile-time checking
}
```

### Record Creation for Transformations

```java
// ✅ Create specific record types for each transformation
public record EmployeeSummary(String name, String department, double salary) {}
public record DepartmentStats(String department, long count, double avgSalary) {}

Dataset<EmployeeSummary> summaries = employees
    .map(emp -> new EmployeeSummary(emp.fullName(), emp.department(), emp.salary()));

Dataset<DepartmentStats> stats = employees
    .groupBy(Employee::department)
    .map(group -> new DepartmentStats(
        group.getKey(),
        group.getData().size(),
        group.getData().stream().mapToDouble(Employee::salary).average().orElse(0.0)
    ));

// ❌ Don't use Object[] or loose typing
Dataset<Object[]> summaries = employees  // Don't do this
    .map(emp -> new Object[]{emp.fullName(), emp.department(), emp.salary()});
```

## Field Selection Best Practices

### Generated Constants Usage

```java
// ✅ Use specific field constants
.fields(Employee.Fields.ID, Employee.Fields.FULL_NAME, Employee.Fields.EMAIL)

// ✅ Use generated arrays for bulk operations
.fieldsArray(Employee.Fields.ALL_FIELDS)
.fieldsArray(Employee.Fields.ANNOTATED_FIELDS)

// ✅ Use column constants when working with display names
.columns(Employee.Cols.COL_ID, Employee.Cols.COL_FULL_NAME)
.columnsArray(Employee.Cols.ALL_COLUMNS)

// ❌ Never mix field names and column names
.fields(Employee.Fields.ID, "Full Name")  // Inconsistent - don't do this
```

### Advanced Selection Patterns

```java
// ✅ Combine selection methods for complex scenarios
List<FieldMeta> exportFields = FieldSelector.from(metadata)
    .fieldsArray(Employee.Fields.ANNOTATED_FIELDS)  // Start with annotated fields
    .exclude(Employee.Fields.INTERNAL_NOTES)        // Remove sensitive fields
    .where(field -> field.getWidth() > 0)           // Only fields with width specified
    .requiredOnly()                                 // Only required fields
    .orderByAnnotation()                            // Order by @DataColumn order
    .select();

// ✅ Conditional field selection based on context
public List<FieldMeta> getFieldsForContext(PojoMetadata<Employee> metadata, ExportContext context) {
    FieldSelector<Employee> selector = FieldSelector.from(metadata);
    
    switch (context) {
        case PUBLIC_REPORT -> selector
            .fieldsArray(Employee.Fields.ANNOTATED_FIELDS)
            .exclude(Employee.Fields.SALARY, Employee.Fields.INTERNAL_NOTES)
            .exportableOnly();
        case INTERNAL_ANALYSIS -> selector
            .fieldsArray(Employee.Fields.ALL_FIELDS)
            .where(field -> !field.isIgnored());
        case SUMMARY -> selector
            .fields(Employee.Fields.ID, Employee.Fields.FULL_NAME, Employee.Fields.DEPARTMENT);
    }
    
    return selector.select();
}
```

## Error Handling Patterns

### File I/O Error Handling

```java
// ✅ Comprehensive error handling for file operations
public Dataset<Employee> readEmployeeData(String filePath) {
    try {
        return ExcelDatasetReader
            .fromFile(filePath)  // Automatic path validation
            .sheet("Employees")
            .hasHeaders(true)
            .read(Employee.class);
    } catch (SecurityException e) {
        // Path traversal or security issue
        throw new IllegalArgumentException("Invalid file path: " + e.getMessage(), e);
    } catch (IOException e) {
        // File reading error
        throw new RuntimeException("Failed to read file '" + filePath + "': " + e.getMessage(), e);
    } catch (RuntimeException e) {
        // Data parsing or validation error
        throw new RuntimeException("Failed to parse employee data: " + e.getMessage(), e);
    }
}

// ✅ Validate data before export
public void exportEmployees(Dataset<Employee> employees, String filePath) {
    if (employees.isEmpty()) {
        throw new IllegalArgumentException("Cannot export empty dataset");
    }
    
    PojoMetadata<Employee> metadata = MetadataCache.getMetadata(Employee.class);
    List<FieldMeta> exportFields = FieldSelector.from(metadata)
        .exportableOnly()
        .select();
        
    if (exportFields.isEmpty()) {
        throw new IllegalStateException("No exportable fields found for Employee");
    }
    
    try {
        ExcelDatasetWriter
            .toFile(filePath)  // Automatic path validation
            .headers(true)
            .fieldsArray(getAllExportableFieldConstants(metadata))
            .write(employees);
    } catch (SecurityException e) {
        throw new IllegalArgumentException("Invalid output path: " + e.getMessage(), e);
    } catch (IOException e) {
        throw new RuntimeException("Failed to write Excel file: " + e.getMessage(), e);
    }
}
```

### Validation Patterns

```java
// ✅ Use metadata for validation
public boolean isValidEmployee(Employee employee, PojoMetadata<Employee> metadata) {
    // Check required fields
    for (FieldMeta field : metadata.getRequiredFields()) {
        try {
            Object value = field.getRecordComponent().getAccessor().invoke(employee);
            if (value == null || (value instanceof String str && str.trim().isEmpty())) {
                System.err.printf("Required field '%s' is missing or empty%n", field.getFieldName());
                return false;
            }
        } catch (Exception e) {
            System.err.printf("Error accessing field '%s': %s%n", field.getFieldName(), e.getMessage());
            return false;
        }
    }
    
    // Additional business validation
    if (employee.email() != null && !employee.email().contains("@")) {
        System.err.println("Invalid email format: " + employee.email());
        return false;
    }
    
    return true;
}

// ✅ Filter invalid records with proper error reporting
public Dataset<Employee> validateAndFilter(Dataset<Employee> employees) {
    PojoMetadata<Employee> metadata = MetadataCache.getMetadata(Employee.class);
    List<Employee> invalid = new ArrayList<>();
    
    Dataset<Employee> valid = employees.filter(emp -> {
        boolean isValid = isValidEmployee(emp, metadata);
        if (!isValid) {
            invalid.add(emp);
        }
        return isValid;
    });
    
    if (!invalid.isEmpty()) {
        System.err.printf("Filtered out %d invalid records%n", invalid.size());
    }
    
    return valid;
}
```

## Performance Guidelines

### Efficient Operation Chaining

```java
// ✅ Filter early, transform late
Dataset<EmployeeSummary> result = employees
    .filter(emp -> emp.status().equals("ACTIVE"))        // Reduce dataset size first
    .filter(emp -> emp.department().equals("Engineering")) // Apply specific filters
    .filter(emp -> emp.salary() > 75000)                 // Further reduce
    .map(emp -> new EmployeeSummary(                      // Transform after filtering
        emp.fullName(), 
        emp.department(), 
        emp.salary()
    ))
    .sorted(Comparator.comparing(EmployeeSummary::name)); // Final sorting

// ❌ Don't transform before filtering
Dataset<EmployeeSummary> inefficient = employees
    .map(emp -> new EmployeeSummary(emp.fullName(), emp.department(), emp.salary()))  // Transform full dataset
    .filter(summary -> summary.salary() > 75000);  // Filter after expensive transformation
```

### Memory-Efficient Patterns

```java
// ✅ Use streams for aggregation without intermediate collections
public record SalaryStats(double mean, double max, double min, long count) {}

public SalaryStats calculateSalaryStats(Dataset<Employee> employees) {
    return employees.stream()
        .mapToDouble(Employee::salary)
        .collect(DoubleSummaryStatistics::new,
                DoubleSummaryStatistics::accept,
                DoubleSummaryStatistics::combine)
        .let(stats -> new SalaryStats(
            stats.getAverage(),
            stats.getMax(), 
            stats.getMin(),
            stats.getCount()
        ));
}

// ❌ Avoid creating unnecessary intermediate collections
public SalaryStats inefficientStats(Dataset<Employee> employees) {
    List<Double> salaries = employees.toList().stream()
        .map(Employee::salary)
        .collect(Collectors.toList());  // Unnecessary intermediate collection
    
    return new SalaryStats(
        salaries.stream().mapToDouble(Double::doubleValue).average().orElse(0),
        salaries.stream().mapToDouble(Double::doubleValue).max().orElse(0),
        salaries.stream().mapToDouble(Double::doubleValue).min().orElse(0),
        salaries.size()
    );
}
```

### Bulk Operations

```java
// ✅ Use generated arrays for bulk field operations
public void exportAllDepartmentReports(Map<String, Dataset<Employee>> departmentData) {
    String[] allFields = Employee.Fields.ALL_FIELDS;  // Use generated array
    
    departmentData.parallelStream().forEach((department, employees) -> {
        String filename = String.format("reports/%s_report.xlsx", 
            department.replaceAll("\\s+", "_").toLowerCase());
            
        ExcelDatasetWriter
            .toFile(filename)
            .sheet(department + " Report")
            .fieldsArray(allFields)  // Bulk field selection
            .write(employees);
    });
}
```

## Security Best Practices

### Trust Built-in Security Measures

```java
// ✅ Library handles path validation automatically
public void safeFileOperations(String userProvidedPath, Dataset<Employee> data) {
    // No need for manual path validation - library handles it
    ExcelDatasetWriter
        .toFile(userProvidedPath)  // Automatic path traversal protection
        .write(data);              // File size limits enforced
}

// ✅ Library handles file size limits
public Dataset<Employee> readLargeFile(String filePath) {
    // No need for manual size checking - library handles it
    return ParquetDatasetReader
        .fromFile(filePath)  // Automatic 100MB limit for Parquet
        .read(Employee.class);
}

// ❌ Don't try to bypass security measures
public void unsafeOperation(String path) {
    // Don't try to work around security features
    File file = new File(path);  // Bypasses built-in protection
    // Use library methods instead
}
```

### Input Validation

```java
// ✅ Validate business logic, not file paths (library handles that)
public void validateBusinessRules(Employee employee) {
    if (employee.salary() < 0) {
        throw new IllegalArgumentException("Salary cannot be negative");
    }
    
    if (employee.email() != null && !employee.email().matches("^[^@]+@[^@]+\\.[^@]+$")) {
        throw new IllegalArgumentException("Invalid email format: " + employee.email());
    }
    
    if (employee.fullName() == null || employee.fullName().trim().isEmpty()) {
        throw new IllegalArgumentException("Employee name is required");
    }
}
```

## Testing Guidelines

### Unit Test Patterns

```java
// ✅ Test with realistic data and comprehensive assertions
@Test
void shouldFilterEmployeesByDepartmentAndSalary() {
    // Given
    Dataset<Employee> employees = Dataset.of(
        new Employee("001", "John Doe", "Engineering", 75000),
        new Employee("002", "Jane Smith", "Engineering", 65000),
        new Employee("003", "Bob Johnson", "Marketing", 80000),
        new Employee("004", "Alice Wilson", "Engineering", 85000)
    );
    
    // When
    Dataset<Employee> result = employees
        .filter(emp -> emp.department().equals("Engineering"))
        .filter(emp -> emp.salary() > 70000);
    
    // Then
    assertThat(result.size()).isEqualTo(2);
    assertThat(result.toList())
        .extracting(Employee::id)
        .containsExactly("001", "004");
    assertThat(result.toList())
        .allSatisfy(emp -> {
            assertThat(emp.department()).isEqualTo("Engineering");
            assertThat(emp.salary()).isGreaterThan(70000);
        });
}

// ✅ Test field constants generation
@Test
void shouldHaveCorrectGeneratedConstants() {
    assertThat(Employee.Fields.ID).isEqualTo("id");
    assertThat(Employee.Fields.FULL_NAME).isEqualTo("fullName");
    assertThat(Employee.Fields.ALL_FIELDS)
        .containsExactly("id", "fullName", "department", "salary");
    assertThat(Employee.Cols.COL_ID).isEqualTo("Employee ID");
    assertThat(Employee.Cols.COL_FULL_NAME).isEqualTo("Full Name");
}
```

### Integration Test Patterns

```java
// ✅ Test complete workflows
@Test
void shouldExportAndReadEmployeeData(@TempDir Path tempDir) throws IOException {
    // Given
    Path excelFile = tempDir.resolve("test_employees.xlsx");
    Dataset<Employee> originalData = Dataset.of(
        new Employee("001", "John Doe", "Engineering", 75000),
        new Employee("002", "Jane Smith", "Marketing", 65000)
    );
    
    // When - Export
    ExcelDatasetWriter
        .toFile(excelFile.toString())
        .fieldsArray(Employee.Fields.ALL_FIELDS)
        .write(originalData);
    
    // When - Read back
    Dataset<Employee> readData = ExcelDatasetReader
        .fromFile(excelFile.toString())
        .read(Employee.class);
    
    // Then
    assertThat(readData.size()).isEqualTo(originalData.size());
    assertThat(readData.toList())
        .containsExactlyInAnyOrderElementsOf(originalData.toList());
}
```

This guide provides AI assistants with comprehensive patterns for generating robust, secure, and efficient Dataset4J code while following library best practices.