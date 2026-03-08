# Dataset4J - AI Coding Assistant Instructions

## Overview

Dataset4J is a Java library providing a pandas-like DataFrame API for Java records, with strong typing, annotation-driven metadata, and support for Excel/Parquet I/O operations. This document provides comprehensive guidance for AI coding assistants working with this library.

## Library Architecture

### Core Modules

```
dataset4j/
├── dataset4j-core/          # Core DataFrame API and annotations
├── dataset4j-poi/           # Excel I/O using Apache POI
├── dataset4j-parquet/       # Parquet I/O (lightweight, no Hadoop)
└── dataset4j/              # Convenience aggregate module
```

### Key Dependencies

- **Java 17+** (Records, Pattern Matching, Text Blocks)
- **Apache POI** (Excel support)
- **Compression Libraries**: Snappy, LZ4, GZIP (Parquet)
- **Jakarta Validation** (Bean validation)

## Core Concepts

### 1. Dataset<T> - Immutable DataFrame

```java
// Core immutable DataFrame class
public final class Dataset<T> {
    private final List<T> rows;  // Immutable record collection
    
    // Fluent API methods return new Dataset instances
    public <R> Dataset<R> map(Function<T, R> mapper);
    public Dataset<T> filter(Predicate<T> predicate);
    public <K> Dataset<T> groupBy(Function<T, K> keyExtractor);
    // ... more operations
}
```

**Key Principles:**
- Immutable - all operations return new instances
- Type-safe - leverages Java records and generics
- Fluent API - chainable operations like pandas
- Lazy evaluation where beneficial

### 2. Record-Based Data Model

```java
// Example record with annotations
@GenerateFields(className = "Fields", columnsClassName = "Cols")
@DataTable(name = "Employee Report")
public record Employee(
    @DataColumn(name = "Employee ID", order = 1, required = true)
    String id,
    
    @DataColumn(name = "First Name", order = 2, width = 20)
    String firstName,
    
    @DataColumn(name = "Email", order = 3, cellType = DataColumn.CellType.TEXT)
    String email,
    
    @DataColumn(ignore = true)
    String internalNotes
) {}
```

### 3. Compile-Time Field Constants Generation

The `@GenerateFields` annotation triggers compile-time generation of static constants:

```java
// Generated at compile time
public static final class Fields {
    public static final String ID = "id";
    public static final String FIRST_NAME = "firstName";
    public static final String EMAIL = "email";
    public static final String[] ALL_FIELDS = {ID, FIRST_NAME, EMAIL};
}

public static final class Cols {
    public static final String COL_ID = "Employee ID";
    public static final String COL_FIRST_NAME = "First Name"; 
    public static final String COL_EMAIL = "Email";
    public static final String[] ALL_COLUMNS = {COL_ID, COL_FIRST_NAME, COL_EMAIL};
}
```

### 4. Metadata System

```java
// Runtime metadata extraction and caching
PojoMetadata<Employee> metadata = MetadataCache.getMetadata(Employee.class);
List<FieldMeta> fields = metadata.getAllFields();
List<FieldMeta> exportableFields = metadata.getExportableFields();
```

## Common Usage Patterns

### Basic Dataset Operations

```java
// Creating datasets
Dataset<Employee> employees = Dataset.of(
    new Employee("001", "John", "john@example.com", "Internal note"),
    new Employee("002", "Jane", "jane@example.com", "Internal note")
);

// Filtering and mapping
Dataset<String> emails = employees
    .filter(emp -> emp.firstName().startsWith("J"))
    .map(Employee::email);

// Grouping and aggregation
Map<String, List<Employee>> byDepartment = employees
    .groupBy(Employee::department)
    .collect(Collectors.toMap(
        group -> group.getKey(),
        group -> group.getData().toList()
    ));
```

### Field Selection API

```java
// Type-safe field selection with generated constants
List<FieldMeta> selectedFields = FieldSelector.from(metadata)
    .fields(Employee.Fields.ID, Employee.Fields.FIRST_NAME, Employee.Fields.EMAIL)
    .exclude(Employee.Fields.INTERNAL_NOTES)
    .requiredOnly()
    .orderByAnnotation()
    .select();

// Bulk operations with generated arrays
List<FieldMeta> allFields = FieldSelector.from(metadata)
    .fieldsArray(Employee.Fields.ALL_FIELDS)
    .select();

// Column-based selection
List<FieldMeta> columnFields = FieldSelector.from(metadata)
    .columns(Employee.Cols.COL_ID, Employee.Cols.COL_FIRST_NAME)
    .select();
```

### Excel I/O Operations

```java
// Reading Excel files
Dataset<Employee> employees = ExcelDatasetReader
    .fromFile("employees.xlsx")
    .sheet("Sheet1")
    .hasHeaders(true)
    .read(Employee.class);

// Writing Excel files with field selection
ExcelDatasetWriter
    .toFile("report.xlsx")
    .sheet("Employee Report")
    .headers(true)
    .autoSize(true)
    .fields(Employee.Fields.ID, Employee.Fields.FIRST_NAME, Employee.Fields.EMAIL)
    .write(employees);

// Using generated constants for bulk export
ExcelDatasetWriter
    .toFile("full-report.xlsx")
    .fieldsArray(Employee.Fields.ALL_FIELDS)
    .write(employees);
```

### Parquet I/O Operations

```java
// Writing Parquet files (lightweight implementation)
ParquetDatasetWriter
    .toFile("employees.parquet")
    .compression(ParquetCompressionCodec.SNAPPY)
    .write(employees);

// Reading Parquet files
Dataset<Employee> employees = ParquetDatasetReader
    .fromFile("employees.parquet")
    .compression(ParquetCompressionCodec.SNAPPY)
    .read(Employee.class);
```

## Pandas DataFrame to Dataset4J Mapping

This section provides a comprehensive mapping between pandas DataFrame operations and their Dataset4J equivalents, enabling smooth transition for developers familiar with pandas.

### Data Creation and Loading

| Pandas | Dataset4J | Description |
|--------|-----------|-------------|
| `pd.DataFrame(data)` | `Dataset.of(records...)` | Create from data |
| `pd.read_csv("file.csv")` | `CsvDatasetReader.fromFile("file.csv").read(RecordClass.class)` | Load from CSV |
| `pd.read_excel("file.xlsx")` | `ExcelDatasetReader.fromFile("file.xlsx").read(RecordClass.class)` | Load from Excel |
| `pd.read_parquet("file.parquet")` | `ParquetDatasetReader.fromFile("file.parquet").read(RecordClass.class)` | Load from Parquet |

```python
# Pandas
df = pd.DataFrame([
    {'name': 'John', 'age': 30, 'city': 'NYC'},
    {'name': 'Jane', 'age': 25, 'city': 'LA'}
])
```

```java
// Dataset4J
public record Person(String name, int age, String city) {}

Dataset<Person> dataset = Dataset.of(
    new Person("John", 30, "NYC"),
    new Person("Jane", 25, "LA")
);
```

### Basic Operations

| Pandas | Dataset4J | Description |
|--------|-----------|-------------|
| `df.shape` | `dataset.size()` | Get row count |
| `df.columns` | `metadata.getAllFields().stream().map(FieldMeta::getFieldName)` | Get column names |
| `df.head(n)` | `dataset.limit(n)` | Get first n rows |
| `df.tail(n)` | `dataset.takeLast(n)` | Get last n rows |
| `df.info()` | `metadata.getAllFields()` | Get column info |
| `df.describe()` | `dataset.aggregate(...)` | Statistical summary |

```python
# Pandas
print(df.shape)  # (2, 3)
print(df.columns.tolist())  # ['name', 'age', 'city']
first_row = df.head(1)
```

```java
// Dataset4J
int rowCount = dataset.size();  // 2
PojoMetadata<Person> metadata = MetadataCache.getMetadata(Person.class);
List<String> columns = metadata.getAllFields().stream()
    .map(FieldMeta::getFieldName)
    .collect(Collectors.toList());  // ["name", "age", "city"]
Dataset<Person> firstRow = dataset.limit(1);
```

### Filtering and Selection

| Pandas | Dataset4J | Description |
|--------|-----------|-------------|
| `df[df['age'] > 25]` | `dataset.filter(p -> p.age() > 25)` | Filter rows |
| `df[df['city'].isin(['NYC', 'LA'])]` | `dataset.filter(p -> Set.of("NYC", "LA").contains(p.city()))` | Filter by values |
| `df[['name', 'age']]` | Field selection via projection | Select columns |
| `df.loc[df['age'] > 25, ['name']]` | Combined filter and select | Filter and select |

```python
# Pandas
adults = df[df['age'] > 25]
names_and_ages = df[['name', 'age']]
filtered_names = df.loc[df['age'] > 25, ['name']]
```

```java
// Dataset4J
Dataset<Person> adults = dataset.filter(p -> p.age() > 25);

// Column selection requires projection to new record type
public record NameAge(String name, int age) {}
Dataset<NameAge> namesAndAges = dataset.map(p -> new NameAge(p.name(), p.age()));

// Combined filter and project
Dataset<String> filteredNames = dataset
    .filter(p -> p.age() > 25)
    .map(Person::name);
```

### Column Operations

| Pandas | Dataset4J | Description |
|--------|-----------|-------------|
| `df['new_col'] = df['age'] * 2` | `dataset.map(p -> new PersonWithDouble(p.name(), p.age(), p.city(), p.age() * 2))` | Add column |
| `df.drop(['col'], axis=1)` | Field selection without dropped column | Drop columns |
| `df.rename(columns={'old': 'new'})` | Create new record type with renamed field | Rename columns |
| `df['age'].apply(lambda x: x * 2)` | `dataset.map(p -> p.age() * 2)` | Transform column |

```python
# Pandas
df['age_doubled'] = df['age'] * 2
df_renamed = df.rename(columns={'age': 'years'})
```

```java
// Dataset4J - Adding column requires new record type
public record PersonWithDouble(String name, int age, String city, int ageDoubled) {}
Dataset<PersonWithDouble> withDoubled = dataset
    .map(p -> new PersonWithDouble(p.name(), p.age(), p.city(), p.age() * 2));

// Renaming requires new record type
public record PersonRenamed(String name, int years, String city) {}
Dataset<PersonRenamed> renamed = dataset
    .map(p -> new PersonRenamed(p.name(), p.age(), p.city()));
```

### Grouping and Aggregation

| Pandas | Dataset4J | Description |
|--------|-----------|-------------|
| `df.groupby('city')` | `dataset.groupBy(Person::city)` | Group by column |
| `df.groupby('city').size()` | `dataset.groupBy(Person::city).map(g -> g.size())` | Count by group |
| `df.groupby('city')['age'].mean()` | `dataset.groupBy(Person::city).map(g -> g.getData().mapToDouble(Person::age).average())` | Mean by group |
| `df.groupby('city').agg({'age': 'mean'})` | Custom aggregation with collectors | Complex aggregation |

```python
# Pandas
city_counts = df.groupby('city').size()
avg_age_by_city = df.groupby('city')['age'].mean()
```

```java
// Dataset4J
public record CityCount(String city, long count) {}
Dataset<CityCount> cityCounts = dataset
    .groupBy(Person::city)
    .map(group -> new CityCount(group.getKey(), group.getData().size()));

public record CityAgeAvg(String city, double avgAge) {}
Dataset<CityAgeAvg> avgAgeByCity = dataset
    .groupBy(Person::city)
    .map(group -> new CityAgeAvg(
        group.getKey(),
        group.getData().stream().mapToInt(Person::age).average().orElse(0.0)
    ));
```

### Joining Operations

| Pandas | Dataset4J | Description |
|--------|-----------|-------------|
| `df1.merge(df2, on='key')` | `dataset1.innerJoin(dataset2, keyExtractor1, keyExtractor2)` | Inner join |
| `df1.merge(df2, on='key', how='left')` | `dataset1.leftJoin(dataset2, keyExtractor1, keyExtractor2)` | Left join |
| `df1.merge(df2, on='key', how='right')` | `dataset1.rightJoin(dataset2, keyExtractor1, keyExtractor2)` | Right join |
| `df1.merge(df2, on='key', how='outer')` | `dataset1.fullOuterJoin(dataset2, keyExtractor1, keyExtractor2)` | Outer join |

```python
# Pandas
df_employees = pd.DataFrame([{'emp_id': 1, 'name': 'John'}, {'emp_id': 2, 'name': 'Jane'}])
df_salaries = pd.DataFrame([{'emp_id': 1, 'salary': 50000}, {'emp_id': 2, 'salary': 60000}])
joined = df_employees.merge(df_salaries, on='emp_id')
```

```java
// Dataset4J
public record Employee(int empId, String name) {}
public record Salary(int empId, double salary) {}
public record EmployeeSalary(int empId, String name, double salary) {}

Dataset<Employee> employees = Dataset.of(
    new Employee(1, "John"),
    new Employee(2, "Jane")
);
Dataset<Salary> salaries = Dataset.of(
    new Salary(1, 50000),
    new Salary(2, 60000)
);

Dataset<EmployeeSalary> joined = employees
    .innerJoin(salaries, Employee::empId, Salary::empId)
    .map(pair -> new EmployeeSalary(
        pair.left().empId(),
        pair.left().name(),
        pair.right().salary()
    ));
```

### Sorting Operations

| Pandas | Dataset4J | Description |
|--------|-----------|-------------|
| `df.sort_values('age')` | `dataset.sorted(Comparator.comparing(Person::age))` | Sort by column |
| `df.sort_values('age', ascending=False)` | `dataset.sorted(Comparator.comparing(Person::age).reversed())` | Sort descending |
| `df.sort_values(['city', 'age'])` | `dataset.sorted(Comparator.comparing(Person::city).thenComparing(Person::age))` | Multi-column sort |

```python
# Pandas
sorted_by_age = df.sort_values('age')
sorted_desc = df.sort_values('age', ascending=False)
sorted_multi = df.sort_values(['city', 'age'])
```

```java
// Dataset4J
Dataset<Person> sortedByAge = dataset
    .sorted(Comparator.comparing(Person::age));

Dataset<Person> sortedDesc = dataset
    .sorted(Comparator.comparing(Person::age).reversed());

Dataset<Person> sortedMulti = dataset
    .sorted(Comparator.comparing(Person::city)
           .thenComparing(Person::age));
```

### Statistical Operations

| Pandas | Dataset4J | Description |
|--------|-----------|-------------|
| `df['age'].sum()` | `dataset.stream().mapToInt(Person::age).sum()` | Sum column |
| `df['age'].mean()` | `dataset.stream().mapToInt(Person::age).average().orElse(0)` | Mean column |
| `df['age'].max()` | `dataset.stream().mapToInt(Person::age).max().orElse(0)` | Max column |
| `df['age'].min()` | `dataset.stream().mapToInt(Person::age).min().orElse(0)` | Min column |
| `df['age'].count()` | `dataset.size()` | Count non-null |

```python
# Pandas
age_stats = {
    'sum': df['age'].sum(),
    'mean': df['age'].mean(),
    'max': df['age'].max(),
    'min': df['age'].min()
}
```

```java
// Dataset4J
public record AgeStats(int sum, double mean, int max, int min) {}
AgeStats ageStats = new AgeStats(
    dataset.stream().mapToInt(Person::age).sum(),
    dataset.stream().mapToInt(Person::age).average().orElse(0),
    dataset.stream().mapToInt(Person::age).max().orElse(0),
    dataset.stream().mapToInt(Person::age).min().orElse(0)
);
```

### Data Export

| Pandas | Dataset4J | Description |
|--------|-----------|-------------|
| `df.to_csv('file.csv')` | `CsvDatasetWriter.toFile('file.csv').write(dataset)` | Export to CSV |
| `df.to_excel('file.xlsx')` | `ExcelDatasetWriter.toFile('file.xlsx').write(dataset)` | Export to Excel |
| `df.to_parquet('file.parquet')` | `ParquetDatasetWriter.toFile('file.parquet').write(dataset)` | Export to Parquet |

```python
# Pandas
df.to_excel('report.xlsx', index=False)
df[['name', 'age']].to_csv('names_ages.csv')
```

```java
// Dataset4J
ExcelDatasetWriter
    .toFile("report.xlsx")
    .headers(true)
    .write(dataset);

// Column selection with generated constants
ExcelDatasetWriter
    .toFile("names_ages.xlsx")
    .fields(Person.Fields.NAME, Person.Fields.AGE)
    .write(dataset);
```

### Key Differences and Design Philosophy

| Aspect | Pandas | Dataset4J |
|--------|--------|-----------|
| **Type Safety** | Dynamic typing | Compile-time type safety with records |
| **Mutability** | Mutable DataFrames | Immutable Dataset operations |
| **Schema** | Dynamic column addition/removal | Fixed schema via record types |
| **Column Selection** | String-based column names | Generated type-safe constants |
| **Performance** | NumPy-backed operations | JVM optimization with streams |
| **Memory Model** | In-memory with copy-on-write | Immutable with structural sharing |

### Migration Tips

**When migrating from pandas to Dataset4J:**

1. **Define Record Types First**: Create Java records that represent your data structure
2. **Use Generated Constants**: Replace string column names with generated field constants
3. **Think Immutably**: Each operation returns a new Dataset instead of modifying existing
4. **Leverage Type Safety**: Let the compiler catch errors instead of runtime checks
5. **Use Projection**: Create new record types for column selection/transformation

**Example Migration:**
```python
# Pandas - Dynamic and mutable
df = pd.read_csv('data.csv')
df['total'] = df['price'] * df['quantity']
result = df[df['total'] > 100][['name', 'total']]
```

```java
// Dataset4J - Type-safe and immutable
@GenerateFields
public record Product(String name, double price, int quantity) {}
public record ProductWithTotal(String name, double price, int quantity, double total) {}
public record NameTotal(String name, double total) {}

Dataset<Product> products = CsvDatasetReader
    .fromFile("data.csv")
    .read(Product.class);

Dataset<NameTotal> result = products
    .map(p -> new ProductWithTotal(p.name(), p.price(), p.quantity(), p.price() * p.quantity()))
    .filter(p -> p.total() > 100)
    .map(p -> new NameTotal(p.name(), p.total()));
```

This mapping helps developers familiar with pandas understand how to accomplish the same tasks in Dataset4J while leveraging Java's type safety and immutability benefits.

## AI Assistant Guidelines

### 1. Code Generation Best Practices

**Always Use:**
- Java records for data structures
- Generated field constants instead of magic strings
- Immutable Dataset operations (fluent API)
- Annotation-driven configuration

**Example - Good:**
```java
// ✅ Good: Using generated constants and fluent API
Dataset<Employee> filtered = employees
    .filter(emp -> emp.salary() > 50000)
    .select(Employee.Fields.ID, Employee.Fields.NAME, Employee.Fields.SALARY);
```

**Example - Avoid:**
```java
// ❌ Avoid: Magic strings and mutable operations
List<Employee> list = employees.toList(); // Don't mutate original
list.removeIf(emp -> emp.salary() <= 50000); // Breaks immutability
```

### 2. Annotation Usage

**Data Structure Annotations:**
```java
@GenerateFields(
    className = "Fields",      // Field constants class name
    columnsClassName = "Cols", // Column constants class name  
    fieldPrefix = "",          // Prefix for field constants
    columnPrefix = "COL_"      // Prefix for column constants
)
@DataTable(name = "Display Name")
public record MyRecord(
    @DataColumn(
        name = "Display Name",           // Column header
        order = 1,                      // Sort order
        required = true,                // Validation
        width = 20,                     // Excel column width
        cellType = DataColumn.CellType.TEXT,  // Cell formatting
        numberFormat = "#,##0.00",      // Number formatting
        dateFormat = "yyyy-MM-dd"       // Date formatting
    )
    String field
) {}
```

### 3. Error Handling Patterns

**File I/O Security:**
```java
// ✅ Library handles security automatically
Dataset<Employee> data = ExcelDatasetReader
    .fromFile(userProvidedPath)  // Path traversal protection built-in
    .read(Employee.class);       // File size limits enforced
```

**Exception Handling:**
```java
try {
    Dataset<Employee> employees = ExcelDatasetReader
        .fromFile("data.xlsx")
        .read(Employee.class);
} catch (SecurityException e) {
    // Path traversal or security issue
} catch (IOException e) {
    // File reading error
} catch (RuntimeException e) {
    // Data parsing or validation error
}
```

### 4. Performance Considerations

**Memory Management:**
```java
// ✅ Good: Chain operations for efficiency
Dataset<Summary> summaries = largeDataset
    .filter(record -> record.isActive())     // Filter early
    .map(this::transformToSummary)           // Transform once
    .groupBy(Summary::department)            // Group efficiently
    .map(this::aggregateGroup);              // Final transformation

// ❌ Avoid: Creating intermediate collections
List<Record> filtered = largeDataset.toList().stream()
    .filter(record -> record.isActive())
    .collect(Collectors.toList()); // Unnecessary intermediate collection
```

**Bulk Operations:**
```java
// ✅ Use generated arrays for bulk field selection
FieldSelector.from(metadata)
    .fieldsArray(Employee.Fields.ALL_FIELDS)  // Efficient bulk selection
    .select();
```

### 5. Type Safety Guidelines

**Generic Type Preservation:**
```java
// ✅ Maintain type information
public <T> Dataset<T> processData(Dataset<T> input, Class<T> recordClass) {
    PojoMetadata<T> metadata = MetadataCache.getMetadata(recordClass);
    return input.filter(createValidationPredicate(metadata));
}

// ✅ Use bounded type parameters when appropriate
public <T extends Record> void exportToExcel(Dataset<T> data, String filename) {
    ExcelDatasetWriter.toFile(filename).write(data);
}
```

### 6. Field Selection Patterns

**Basic Selection:**
```java
// Select specific fields
.fields(Employee.Fields.ID, Employee.Fields.NAME)

// Select by column names
.columns(Employee.Cols.COL_ID, Employee.Cols.COL_NAME)

// Exclude fields
.exclude(Employee.Fields.INTERNAL_NOTES)

// Conditional selection
.requiredOnly()          // Only required fields
.exportableOnly()        // Only non-ignored fields
.ofType(String.class)    // Fields of specific type
.withFormatting()        // Fields with formatting rules
```

**Advanced Selection:**
```java
// Complex selection with chaining
List<FieldMeta> fields = FieldSelector.from(metadata)
    .fields(Employee.Fields.ID, Employee.Fields.NAME)
    .exclude(Employee.Fields.SALARY)
    .where(field -> field.getWidth() > 0)
    .orderByAnnotation()
    .select();
```

## Common Anti-Patterns to Avoid

### 1. Direct List Manipulation
```java
// ❌ Don't do this
List<Employee> employees = dataset.toList();
employees.add(newEmployee); // Breaks immutability

// ✅ Do this instead
Dataset<Employee> updated = dataset.append(newEmployee);
```

### 2. Magic Strings
```java
// ❌ Avoid magic strings
.fields("id", "name", "email")

// ✅ Use generated constants
.fields(Employee.Fields.ID, Employee.Fields.NAME, Employee.Fields.EMAIL)
```

### 3. Manual Metadata Creation
```java
// ❌ Don't create metadata manually
FieldMeta field = new FieldMeta(...); // Complex and error-prone

// ✅ Use annotation-driven metadata
PojoMetadata<Employee> metadata = MetadataCache.getMetadata(Employee.class);
```

## Troubleshooting Common Issues

### 1. Annotation Processing
- Ensure `@GenerateFields` is on record classes only
- Check that annotation processor is enabled in IDE
- Verify generated constants are available after compilation

### 2. Field Selection
- Use `fieldsArray()` and `columnsArray()` for array parameters
- Check field names match record component names exactly
- Verify metadata is cached correctly

### 3. I/O Operations
- File paths are automatically validated for security
- File size limits are enforced (100MB for Parquet)
- Use appropriate compression for Parquet files

## Version Compatibility

- **Java Version**: Requires Java 17+ (uses Records, Pattern Matching)
- **Maven**: Compatible with Maven 3.6+
- **Dependencies**: All managed through Maven, no manual classpath setup needed

## Security Considerations

The library includes built-in security measures:
- ✅ Path traversal protection on all file operations
- ✅ File size limits to prevent DoS attacks
- ✅ Thread-safe operations throughout
- ✅ Input validation on format strings

When using the library, AI assistants should:
- Trust the built-in security measures
- Not attempt to bypass security checks
- Report any security concerns in generated code

## Testing Recommendations

```java
// Example test patterns
@Test
void shouldFilterAndMapEmployees() {
    Dataset<Employee> employees = Dataset.of(
        new Employee("001", "John", "john@example.com", ""),
        new Employee("002", "Jane", "jane@example.com", "")
    );
    
    Dataset<String> emails = employees
        .filter(emp -> emp.firstName().startsWith("J"))
        .map(Employee::email);
        
    assertThat(emails.size()).isEqualTo(2);
    assertThat(emails.toList()).containsExactly("john@example.com", "jane@example.com");
}
```

This instruction file provides AI assistants with comprehensive guidance for working effectively with the dataset4j library while following best practices and avoiding common pitfalls.