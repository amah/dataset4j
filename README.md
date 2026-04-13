# Dataset4J

A modular, lightweight DataFrame-like library for Java records. Port your pandas data processing pipelines to Java with strong typing, fluent APIs, and comprehensive format support.

## 🚀 **Quick Start**

### Maven Dependency

**Option 1: Complete Library** (Recommended)
```xml
<dependency>
    <groupId>io.github.amah</groupId>
    <artifactId>dataset4j</artifactId>
    <version>1.0.12</version>
</dependency>
```

**Option 2: Modular Dependencies**
```xml
<!-- Core functionality only -->
<dependency>
    <groupId>io.github.amah</groupId>
    <artifactId>dataset4j-core</artifactId>
    <version>1.0.12</version>
</dependency>

<!-- Add Excel/CSV support -->
<dependency>
    <groupId>io.github.amah</groupId>
    <artifactId>dataset4j-poi</artifactId>
    <version>1.0.12</version>
</dependency>

<!-- Add Parquet support (lightweight, no Hadoop) -->
<dependency>
    <groupId>io.github.amah</groupId>
    <artifactId>dataset4j-parquet</artifactId>
    <version>1.0.12</version>
</dependency>
```

### Basic Usage

```java
// Define your data structure with annotations
@GenerateFields(className = "Fields", columnsClassName = "Cols")
public record Employee(
    @DataColumn(name = "Employee ID", order = 1, required = true)
    String id,
    @DataColumn(name = "Full Name", order = 2)
    String fullName,
    @DataColumn(name = "Email", order = 3)
    String email,
    @DataColumn(name = "Department", order = 4)
    String department
) {}

// Create and process datasets with pandas-like operations
Dataset<Employee> employees = Dataset.of(
    new Employee("001", "John Doe", "john@company.com", "Engineering"),
    new Employee("002", "Jane Smith", "jane@company.com", "Marketing")
);

// Filter, transform, and aggregate data
Dataset<String> engineerEmails = employees
    .filter(emp -> emp.department().equals("Engineering"))
    .map(Employee::email);

// Export to Excel with type-safe field selection
ExcelDatasetWriter
    .toFile("report.xlsx")
    .fields(Employee.Fields.ID, Employee.Fields.FULL_NAME, Employee.Fields.EMAIL)
    .write(employees);

// Read from Excel
Dataset<Employee> fromExcel = ExcelDatasetReader
    .fromFile("employees.xlsx")
    .read(Employee.class);

// High-performance Parquet I/O
ParquetDatasetWriter
    .toFile("data.parquet")
    .compression(ParquetCompressionCodec.SNAPPY)
    .write(employees);
```

## ✨ **Key Features**

### 🎯 **Type Safety**
- **Java Records**: Strong typing with compile-time validation
- **Generated Constants**: Auto-generated field constants eliminate magic strings
- **IDE Support**: Full auto-completion and refactoring support

### 🐼 **Pandas-Like API**
- **Immutable Operations**: All operations return new Dataset instances
- **Fluent Interface**: Chain operations naturally
- **Familiar Methods**: `filter()`, `map()`, `groupBy()`, `sortBy()`, etc.

### 📊 **Format Support**
- **Excel**: Read/write with Apache POI integration
- **Parquet**: Lightweight implementation (96% size reduction vs Hadoop)
- **CSV**: Read/write with OpenCSV integration
- **JSON**: Planned

### 🏗️ **Modular Architecture**
- **dataset4j-core**: Zero dependencies (50KB)
- **dataset4j-poi**: Excel/CSV support (15MB)
- **dataset4j-parquet**: Parquet support (5.6MB vs 145MB traditional)
- **dataset4j**: All-in-one convenience module

### 🔒 **Security First**
- **Path Validation**: Automatic protection against path traversal
- **Safe Dependencies**: Vetted compression libraries
- **File Size Limits**: Built-in DoS protection

## 📖 **Documentation**

- **[Table (Untyped Data)](docs/TABLE.md)** - Schema-free tabular data with source type preservation
- **[CellValue Coercion](docs/CELLVALUE_COERCION.md)** - Type coercion rules for untyped data
- **[Core Concepts](docs/ai-instructions/dataset4j-ai-instructions-core-concepts.md)** - Understanding Dataset4J fundamentals
- **[Usage Patterns](docs/ai-instructions/dataset4j-ai-instructions-usage-patterns.md)** - Common operations and examples
- **[Pandas Migration](docs/ai-instructions/dataset4j-ai-instructions-pandas-mapping.md)** - Complete pandas to Dataset4J mapping
- **[Architecture](docs/architecture/MODULAR_ARCHITECTURE.md)** - Module structure and deployment options
- **[AI Instructions](docs/ai-instructions/)** - Complete guide for AI coding assistants

## 🧱 **Key Constructs**

### Dataset\<T\> — Immutable DataFrame

The core class wrapping a `List<T>` of Java records with fluent, pandas-like operations. All operations return new instances.

```java
record Employee(String name, int age, String dept) {}

var ds = Dataset.of(
    new Employee("Alice", 30, "Eng"),
    new Employee("Bob",   25, "Sales")
);

ds.filter(e -> e.age() > 25)
  .sortBy(Employee::name)
  .map(Employee::name);       // Dataset<String>
```

### Table — Untyped Tabular Data

Schema-free sibling of `Dataset<T>`. Load files without defining a record class, inspect and reshape data dynamically, then convert to a typed Dataset when ready. Each cell preserves its source type and format metadata via `CellValue`. See **[full documentation](docs/TABLE.md)**.

```java
// Read any file without a record class
Table table = ExcelDatasetReader.fromFile("data.xlsx").readTable();
Table csv   = CsvDatasetReader.fromFile("data.csv").readTable();

// Inspect, filter, reshape
table.columns();                    // ["name", "age", "dept"]
table.cellValue(0, "age").type();   // ValueType.NUMBER
table.cellValue(0, "age").format(); // "$#,##0" (from Excel)

Table result = table
    .filter(row -> row.get("dept").asString().equals("Eng"))
    .select("name", "age")
    .sortBy("age");

// Melt (unpivot) and explode
table.melt(List.of("name"), List.of("Q1","Q2","Q3"), "quarter", "score");
table.explode("tags", cv -> Arrays.stream(cv.asString().split(","))
        .map(CellValue::ofString).collect(Collectors.toList()));

// Convert to typed Dataset
Dataset<Employee> ds = result.toDataset(Employee.class);

// Or write directly to another format
ParquetDatasetWriter.toFile("output.parquet").writeTable(table);
```

### Pair\<L, R\> — Join Result

Generic pair returned by join methods. Provides `left()` / `right()` accessors and utility methods for outer join null-checking.

```java
Dataset<Pair<Employee, Department>> joined =
    employees.innerJoin(departments, Employee::dept, Department::dept);

// Filter and transform without defining custom records
joined.filter(p -> p.left().age() > 25)
      .map(p -> p.left().name() + " in " + p.right().location());

// Null-safe checks for outer joins
joined = employees.leftJoin(departments, Employee::dept, Department::dept);
joined.filter(Pair::hasRight)
      .map(p -> p.right().location());
```

### CompositeKey — Multi-Column Join Keys

Enables joining on multiple fields with proper `equals()` / `hashCode()` semantics. Three API styles:

```java
// Style 1: CompositeKey.of() with *Multi methods
employees.innerJoinMulti(departments,
    e -> CompositeKey.of(e.dept(), e.location()),
    d -> CompositeKey.of(d.dept(), d.location()));

// Style 2: Convenience methods for 2 or 3 keys
employees.innerJoin2(departments,
    Employee::dept, Employee::location,
    Department::dept, Department::location);

// Style 3: Fluent API with CompositeKey.on()
import static dataset4j.CompositeKey.on;

employees.innerJoinOn(departments,
    on(Employee::dept, Employee::location),
    on(Department::dept, Department::location));
```

**Typed accessors** for retrieving components:

```java
CompositeKey key = CompositeKey.of("Engineering", 42, LocalDate.of(2024, 1, 1));
String dept = key.asString(0);         // "Engineering"
Integer count = key.asInteger(1);      // 42
LocalDate date = key.asLocalDate(2);   // 2024-01-01
// Also: asLong(), asDouble(), asBoolean(), asLocalTime(), asLocalDateTime()
// Generic: key.as(0, MyType.class)
```

### GroupedDataset\<K, T\> — GroupBy Result

Returned by `groupBy()`, provides aggregation methods (`counts()`, `sumInt()`, `meanInt()`, `aggregate()`) and group iteration.

```java
var grouped = employees.groupBy(Employee::dept);
grouped.counts();                        // Map<String, Integer>
grouped.meanInt(Employee::age);          // Map<String, Double>
grouped.aggregate((dept, group) ->
    new Stats(dept, group.size()));       // Dataset<Stats>
```

### Join Types

| Method | Description | Null behavior |
|--------|-------------|---------------|
| `innerJoin` | Matching rows only | Neither side null |
| `leftJoin` | All left rows | `right()` may be null |
| `rightJoin` | All right rows | `left()` may be null |
| `crossJoin` | Cartesian product | Neither side null |

Multi-key variants: `innerJoinMulti`, `leftJoinMulti`, `rightJoinMulti`, `innerJoin2`, `leftJoin2`, `innerJoin3Keys`, `innerJoinOn`, `leftJoinOn`, `rightJoinOn`.

## 🤖 **AI Instructions**

Dataset4J ships with comprehensive AI coding assistant instructions in [`docs/ai-instructions/`](docs/ai-instructions/). These guides help AI tools (Copilot, Claude, Cursor, etc.) generate correct Dataset4J code:

| Guide | Purpose |
|-------|---------|
| [Overview](docs/ai-instructions/dataset4j-ai-instructions-overview.md) | High-level orientation for AI assistants |
| [Core Concepts](docs/ai-instructions/dataset4j-ai-instructions-core-concepts.md) | Dataset, records, annotations, metadata, field selection |
| [Usage Patterns](docs/ai-instructions/dataset4j-ai-instructions-usage-patterns.md) | Common operations, joins, I/O, testing patterns |
| [Pandas Mapping](docs/ai-instructions/dataset4j-ai-instructions-pandas-mapping.md) | Complete pandas-to-Dataset4J operation mapping |
| [Guidelines](docs/ai-instructions/dataset4j-ai-instructions-guidelines.md) | Code generation rules and best practices |
| [Coding Instructions](docs/ai-instructions/dataset4j-ai-coding-instructions.md) | Detailed coding conventions |

**Usage**: Copy the relevant instruction files into your AI assistant's context, or reference them in your project's `.cursorrules`, `CLAUDE.md`, or similar configuration.

## 🔧 **Advanced Features**

### Name-Based Column Matching (Excel Reader)

The `order` attribute in `@DataColumn` is now optional. When omitted, columns are matched by header name:

```java
// No order needed — matched by header name
public record Employee(
    @DataColumn(name = "Employee ID") String id,
    @DataColumn(name = "Full Name") String name,
    @DataColumn(name = "Hire Date", dateFormat = "yyyy-MM-dd") LocalDate hireDate
) {}

// Columns can appear in any order in the file
Dataset<Employee> employees = ExcelDatasetReader
    .fromFile("employees.xlsx")
    .readAs(Employee.class);
```

Matching is case-insensitive. If neither `order` nor a matching header is found, the field receives its default value.

### Rich Error Reporting (DatasetReadException)

Parse errors now include the exact cell coordinate, sheet name, and Java field name:

```java
try {
    ExcelDatasetReader.fromFile("data.xlsx").readAs(Employee.class);
} catch (DatasetReadException e) {
    e.getCellReference();       // "Sheet1!C5"
    e.getRow();                 // 4 (0-based)
    e.getColumn();              // 2 (0-based)
    e.getSheetName();           // "Sheet1"
    e.getFieldName();           // "hireDate"
    e.getQualifiedFieldName();  // "Employee.hireDate"
    e.getRawValue();            // "not-a-date"
    // Message: Failed to parse value 'not-a-date' for field 'hireDate'
    //          (java: Employee.hireDate) of type LocalDate at Sheet1!C5 (row 4, column 2)
}
```

### Varargs distinctBy

Deduplicate on any number of keys:

```java
dataset.distinctBy(Row::a, Row::b, Row::c, Row::d, Row::e);
```

The typed overloads for 1-3 keys remain for better type safety.

### Generated Field Constants
```java
@GenerateFields
public record Product(
    @DataColumn(name = "Product ID") String id,
    @DataColumn(name = "Price") double price
) {}

// Auto-generated at compile time:
// Product.Fields.ID = "id"
// Product.Fields.PRICE = "price"  
// Product.Cols.COL_ID = "Product ID"
// Product.Cols.COL_PRICE = "Price"

// Use in field selection
FieldSelector.from(metadata)
    .fields(Product.Fields.ID, Product.Fields.PRICE)
    .select();
```

### Flexible Field Selection
```java
// Conditional field selection
List<FieldMeta> exportFields = FieldSelector.from(metadata)
    .fieldsArray(Employee.Fields.ALL_FIELDS)
    .exclude(Employee.Fields.INTERNAL_NOTES)
    .requiredOnly()
    .exportableOnly()
    .orderByAnnotation()
    .select();

// Use with Excel export
ExcelDatasetWriter
    .toFile("filtered_report.xlsx")
    .select(FieldSelector.from(metadata).where(exportFields::contains))
    .write(dataset);
```

### High-Performance Parquet
```java
// Multiple compression options
ParquetDatasetWriter
    .toFile("data.parquet")
    .compression(ParquetCompressionCodec.SNAPPY)  // Fast & efficient
    .compression(ParquetCompressionCodec.LZ4)     // Fastest
    .compression(ParquetCompressionCodec.GZIP)    // Best compression
    .write(largeDataset);
```

## 🤝 **Contributing**

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

## 📄 **License**

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 **Links**

- **Maven Central**: https://central.sonatype.com/artifact/io.github.amah/dataset4j
- **GitHub**: https://github.com/amah/dataset4j
- **Issues**: https://github.com/amah/dataset4j/issues

---

**Dataset4J v1.0.12** - Bringing pandas-like data processing to Java with type safety and performance.