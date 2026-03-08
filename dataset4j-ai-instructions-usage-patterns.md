# Dataset4J - Common Usage Patterns for AI Assistants

This guide provides practical patterns and examples for common Dataset4J operations that AI assistants should use when generating code.

## Basic Dataset Operations

### Creating Datasets

```java
// From individual records
Dataset<Employee> employees = Dataset.of(
    new Employee("001", "John", "john@example.com"),
    new Employee("002", "Jane", "jane@example.com")
);

// From existing collection
List<Employee> employeeList = Arrays.asList(/* ... */);
Dataset<Employee> dataset = Dataset.from(employeeList);

// Empty dataset
Dataset<Employee> empty = Dataset.empty();
```

### Filtering and Transformation

```java
// Simple filtering
Dataset<Employee> activeEmployees = employees
    .filter(emp -> emp.status().equals("ACTIVE"));

// Complex filtering with multiple conditions
Dataset<Employee> filtered = employees
    .filter(emp -> emp.salary() > 50000)
    .filter(emp -> emp.department().equals("Engineering"))
    .filter(emp -> emp.startDate().isAfter(LocalDate.of(2020, 1, 1)));

// Transformation with mapping
public record EmployeeSummary(String name, String department, double salary) {}
Dataset<EmployeeSummary> summaries = employees
    .map(emp -> new EmployeeSummary(emp.fullName(), emp.department(), emp.salary()));

// Method reference mapping
Dataset<String> emails = employees.map(Employee::email);
```

### Combining Operations

```java
// Chain multiple operations
Dataset<String> result = employees
    .filter(emp -> emp.department().equals("Engineering"))
    .map(Employee::email)
    .sorted(String::compareToIgnoreCase)
    .limit(10);

// Complex transformation pipeline
public record DepartmentStats(String department, long count, double avgSalary) {}
Dataset<DepartmentStats> stats = employees
    .filter(emp -> emp.status().equals("ACTIVE"))
    .groupBy(Employee::department)
    .map(group -> new DepartmentStats(
        group.getKey(),
        group.getData().size(),
        group.getData().stream().mapToDouble(Employee::salary).average().orElse(0.0)
    ));
```

## Field Selection Patterns

### Using Generated Constants

```java
@GenerateFields(className = "Fields", columnsClassName = "Cols")
public record Employee(
    @DataColumn(name = "Employee ID", order = 1) String id,
    @DataColumn(name = "Full Name", order = 2) String fullName,
    @DataColumn(name = "Email", order = 3) String email,
    @DataColumn(name = "Department", order = 4) String department,
    @DataColumn(ignore = true) String internalNotes
) {}

// Basic field selection with generated constants
PojoMetadata<Employee> metadata = MetadataCache.getMetadata(Employee.class);
List<FieldMeta> selectedFields = FieldSelector.from(metadata)
    .fields(Employee.Fields.ID, Employee.Fields.FULL_NAME, Employee.Fields.EMAIL)
    .select();

// Column-based selection
List<FieldMeta> columnFields = FieldSelector.from(metadata)
    .columns(Employee.Cols.COL_ID, Employee.Cols.COL_FULL_NAME)
    .select();
```

### Advanced Field Selection

```java
// Conditional field selection
List<FieldMeta> conditionalFields = FieldSelector.from(metadata)
    .fields(Employee.Fields.ID, Employee.Fields.FULL_NAME)
    .exclude(Employee.Fields.INTERNAL_NOTES)
    .where(field -> field.getWidth() > 0)
    .requiredOnly()
    .orderByAnnotation()
    .select();

// Type-based selection
List<FieldMeta> stringFields = FieldSelector.from(metadata)
    .ofType(String.class)
    .exportableOnly()
    .select();

// Bulk operations with arrays
List<FieldMeta> allFields = FieldSelector.from(metadata)
    .fieldsArray(Employee.Fields.ALL_FIELDS)
    .select();

List<FieldMeta> allColumns = FieldSelector.from(metadata)
    .columnsArray(Employee.Cols.ALL_COLUMNS)
    .select();
```

## Excel I/O Patterns

### Reading Excel Files

```java
// Basic Excel reading
Dataset<Employee> employees = ExcelDatasetReader
    .fromFile("employees.xlsx")
    .sheet("Employee Data")
    .hasHeaders(true)
    .read(Employee.class);

// Excel reading with configuration
Dataset<Employee> employees = ExcelDatasetReader
    .fromFile("data.xlsx")
    .sheet("Sheet1")
    .hasHeaders(true)
    .startRow(1)
    .read(Employee.class);
```

### Writing Excel Files

```java
// Basic Excel writing
ExcelDatasetWriter
    .toFile("output.xlsx")
    .sheet("Employee Report")
    .headers(true)
    .autoSize(true)
    .write(employees);

// Excel writing with field selection
ExcelDatasetWriter
    .toFile("selected_fields.xlsx")
    .sheet("Report")
    .fields(Employee.Fields.ID, Employee.Fields.FULL_NAME, Employee.Fields.EMAIL)
    .write(employees);

// Excel writing with generated arrays
ExcelDatasetWriter
    .toFile("all_fields.xlsx")
    .fieldsArray(Employee.Fields.ALL_FIELDS)
    .write(employees);

// Excel writing with column selection
ExcelDatasetWriter
    .toFile("columns.xlsx")
    .columns(Employee.Cols.COL_ID, Employee.Cols.COL_FULL_NAME)
    .write(employees);
```

### Advanced Excel Operations

```java
// Multiple sheets
ExcelDatasetWriter writer = ExcelDatasetWriter.toFile("multi_sheet.xlsx");

writer.sheet("All Employees")
      .fieldsArray(Employee.Fields.ALL_FIELDS)
      .write(allEmployees);

writer.sheet("Engineers Only")
      .fields(Employee.Fields.FULL_NAME, Employee.Fields.EMAIL)
      .write(engineers);

// Conditional export based on metadata
List<FieldMeta> exportFields = FieldSelector.from(metadata)
    .exportableOnly()
    .where(field -> !field.getNumberFormat().isEmpty() || !field.getDateFormat().equals("yyyy-MM-dd"))
    .orderByAnnotation()
    .select();

ExcelDatasetWriter
    .toFile("formatted_fields.xlsx")
    .select(FieldSelector.from(metadata).where(field -> exportFields.contains(field)))
    .write(employees);
```

## Parquet I/O Patterns

### Reading and Writing Parquet

```java
// Writing Parquet with compression
ParquetDatasetWriter
    .toFile("employees.parquet")
    .compression(ParquetCompressionCodec.SNAPPY)
    .write(employees);

// Reading Parquet
Dataset<Employee> employees = ParquetDatasetReader
    .fromFile("employees.parquet")
    .compression(ParquetCompressionCodec.SNAPPY)
    .read(Employee.class);

// Different compression options
ParquetDatasetWriter
    .toFile("large_dataset.parquet")
    .compression(ParquetCompressionCodec.LZ4)  // Faster compression
    .write(largeDataset);

ParquetDatasetWriter
    .toFile("archive.parquet")
    .compression(ParquetCompressionCodec.GZIP)  // Better compression ratio
    .write(archiveData);
```

## Grouping and Aggregation Patterns

### Basic Grouping

```java
// Simple grouping with count
public record DepartmentCount(String department, long count) {}
Dataset<DepartmentCount> departmentCounts = employees
    .groupBy(Employee::department)
    .map(group -> new DepartmentCount(group.getKey(), group.getData().size()));

// Grouping with aggregation
public record SalaryStats(String department, double avgSalary, double maxSalary, long count) {}
Dataset<SalaryStats> salaryStats = employees
    .groupBy(Employee::department)
    .map(group -> {
        List<Employee> dept = group.getData().toList();
        return new SalaryStats(
            group.getKey(),
            dept.stream().mapToDouble(Employee::salary).average().orElse(0.0),
            dept.stream().mapToDouble(Employee::salary).max().orElse(0.0),
            dept.size()
        );
    });
```

### Complex Aggregation

```java
// Multi-level grouping
public record LocationDeptStats(String location, String department, long count, double avgSalary) {}
Dataset<LocationDeptStats> locationDeptStats = employees
    .groupBy(emp -> emp.location() + "|" + emp.department())  // Composite key
    .map(group -> {
        String[] parts = group.getKey().split("\\|");
        List<Employee> emps = group.getData().toList();
        return new LocationDeptStats(
            parts[0],
            parts[1], 
            emps.size(),
            emps.stream().mapToDouble(Employee::salary).average().orElse(0.0)
        );
    });

// Using CompositeKey for cleaner multi-level grouping
Dataset<LocationDeptStats> cleanerStats = employees
    .groupBy(emp -> CompositeKey.of(emp.location(), emp.department()))
    .map(group -> {
        CompositeKey key = group.getKey();
        List<Employee> emps = group.getData().toList();
        return new LocationDeptStats(
            key.component(0),
            key.component(1),
            emps.size(),
            emps.stream().mapToDouble(Employee::salary).average().orElse(0.0)
        );
    });
```

## Joining Patterns

### Basic Joins

```java
public record Employee(String id, String name, String deptId) {}
public record Department(String id, String name, String manager) {}
public record EmployeeWithDept(String empId, String empName, String deptName, String manager) {}

// Inner join
Dataset<EmployeeWithDept> joined = employees
    .innerJoin(departments, Employee::deptId, Department::id)
    .map(pair -> new EmployeeWithDept(
        pair.left().id(),
        pair.left().name(),
        pair.right().name(),
        pair.right().manager()
    ));

// Left join (keeps all employees)
Dataset<EmployeeWithDept> leftJoined = employees
    .leftJoin(departments, Employee::deptId, Department::id)
    .map(pair -> new EmployeeWithDept(
        pair.left().id(),
        pair.left().name(),
        pair.right() != null ? pair.right().name() : "Unknown",
        pair.right() != null ? pair.right().manager() : "N/A"
    ));
```

### Complex Joins

```java
// Multiple joins
public record Salary(String empId, double amount) {}
public record FullEmployeeInfo(String id, String name, String deptName, double salary) {}

Dataset<FullEmployeeInfo> fullInfo = employees
    .innerJoin(departments, Employee::deptId, Department::id)
    .map(pair -> new EmployeeWithDept(
        pair.left().id(),
        pair.left().name(), 
        pair.right().name(),
        pair.right().manager()
    ))
    .innerJoin(salaries, EmployeeWithDept::empId, Salary::empId)
    .map(pair -> new FullEmployeeInfo(
        pair.left().empId(),
        pair.left().empName(),
        pair.left().deptName(),
        pair.right().amount()
    ));
```

## Validation and Error Handling Patterns

### Field Validation

```java
// Validation with field metadata
public boolean validateEmployee(Employee employee, PojoMetadata<Employee> metadata) {
    for (FieldMeta field : metadata.getRequiredFields()) {
        try {
            Object value = field.getRecordComponent().getAccessor().invoke(employee);
            if (value == null || (value instanceof String str && str.isEmpty())) {
                System.err.printf("Required field '%s' is missing or empty%n", field.getFieldName());
                return false;
            }
        } catch (Exception e) {
            System.err.printf("Error accessing field '%s': %s%n", field.getFieldName(), e.getMessage());
            return false;
        }
    }
    return true;
}

// Filter valid records
Dataset<Employee> validEmployees = employees
    .filter(emp -> validateEmployee(emp, metadata));
```

### Error Handling in I/O

```java
// Safe Excel reading with error handling
public Dataset<Employee> readEmployeesFromExcel(String filePath) {
    try {
        return ExcelDatasetReader
            .fromFile(filePath)
            .sheet("Employees")
            .hasHeaders(true)
            .read(Employee.class);
    } catch (SecurityException e) {
        throw new IllegalArgumentException("Invalid file path: " + e.getMessage(), e);
    } catch (IOException e) {
        throw new RuntimeException("Failed to read Excel file: " + e.getMessage(), e);
    } catch (Exception e) {
        throw new RuntimeException("Unexpected error reading employee data: " + e.getMessage(), e);
    }
}

// Safe Excel writing with validation
public void writeEmployeesToExcel(Dataset<Employee> employees, String filePath) {
    if (employees.isEmpty()) {
        throw new IllegalArgumentException("Cannot write empty dataset");
    }
    
    try {
        PojoMetadata<Employee> metadata = MetadataCache.getMetadata(Employee.class);
        List<FieldMeta> exportFields = FieldSelector.from(metadata)
            .exportableOnly()
            .orderByAnnotation()
            .select();
            
        if (exportFields.isEmpty()) {
            throw new IllegalStateException("No exportable fields found");
        }
        
        ExcelDatasetWriter
            .toFile(filePath)
            .headers(true)
            .autoSize(true)
            .select(FieldSelector.from(metadata).where(exportFields::contains))
            .write(employees);
            
    } catch (SecurityException e) {
        throw new IllegalArgumentException("Invalid file path: " + e.getMessage(), e);
    } catch (IOException e) {
        throw new RuntimeException("Failed to write Excel file: " + e.getMessage(), e);
    }
}
```

## Performance Optimization Patterns

### Efficient Filtering

```java
// ✅ Good: Filter early in the pipeline
Dataset<Employee> result = employees
    .filter(emp -> emp.status().equals("ACTIVE"))     // Reduce dataset size first
    .filter(emp -> emp.salary() > 50000)              // Then apply more specific filters
    .map(this::enrichEmployeeData)                    // Expensive operations after filtering
    .sorted(Comparator.comparing(Employee::lastName));

// ❌ Avoid: Expensive operations before filtering
Dataset<Employee> inefficient = employees
    .map(this::enrichEmployeeData)                    // Expensive operation on full dataset
    .filter(emp -> emp.status().equals("ACTIVE"))     // Filter after expensive operation
    .filter(emp -> emp.salary() > 50000);
```

### Bulk Operations

```java
// Use generated arrays for bulk field selection
List<FieldMeta> allExportableFields = FieldSelector.from(metadata)
    .fieldsArray(Employee.Fields.ANNOTATED_FIELDS)    // Bulk selection
    .exportableOnly()
    .select();

// Batch I/O operations
public void exportDepartmentReports(Map<String, Dataset<Employee>> departmentData) {
    departmentData.forEach((department, employees) -> {
        String filename = String.format("reports/%s_employees.xlsx", department);
        ExcelDatasetWriter
            .toFile(filename)
            .sheet(department + " Employees")
            .fieldsArray(Employee.Fields.ALL_FIELDS)   // Use bulk selection
            .write(employees);
    });
}
```

### Memory Management

```java
// ✅ Good: Chain operations to avoid intermediate collections
public Dataset<EmployeeSummary> createEmployeeSummaries(Dataset<Employee> employees) {
    return employees
        .filter(emp -> emp.status().equals("ACTIVE"))
        .map(emp -> new EmployeeSummary(emp.id(), emp.fullName(), emp.department()))
        .sorted(Comparator.comparing(EmployeeSummary::name));
}

// ❌ Avoid: Creating intermediate collections
public Dataset<EmployeeSummary> inefficientSummaries(Dataset<Employee> employees) {
    List<Employee> active = employees.toList().stream()
        .filter(emp -> emp.status().equals("ACTIVE"))
        .collect(Collectors.toList());  // Unnecessary intermediate collection
        
    List<EmployeeSummary> summaries = active.stream()
        .map(emp -> new EmployeeSummary(emp.id(), emp.fullName(), emp.department()))
        .collect(Collectors.toList());  // Another intermediate collection
        
    return Dataset.from(summaries);
}
```

## Testing Patterns

### Unit Testing with Datasets

```java
@Test
void shouldFilterActiveEmployees() {
    // Given
    Dataset<Employee> employees = Dataset.of(
        new Employee("001", "John", "ACTIVE", "Engineering"),
        new Employee("002", "Jane", "INACTIVE", "Marketing"),
        new Employee("003", "Bob", "ACTIVE", "Engineering")
    );
    
    // When
    Dataset<Employee> activeEmployees = employees
        .filter(emp -> emp.status().equals("ACTIVE"));
    
    // Then
    assertThat(activeEmployees.size()).isEqualTo(2);
    assertThat(activeEmployees.toList())
        .extracting(Employee::id)
        .containsExactly("001", "003");
}

@Test
void shouldGenerateCorrectFieldConstants() {
    // Given
    PojoMetadata<Employee> metadata = MetadataCache.getMetadata(Employee.class);
    
    // When
    List<String> fieldNames = FieldSelector.from(metadata)
        .fieldsArray(Employee.Fields.ALL_FIELDS)
        .selectNames();
    
    // Then
    assertThat(fieldNames).containsExactly("id", "fullName", "status", "department");
    assertThat(Employee.Fields.ID).isEqualTo("id");
    assertThat(Employee.Fields.FULL_NAME).isEqualTo("fullName");
}
```

These patterns provide AI assistants with proven approaches for generating robust, efficient, and maintainable Dataset4J code.