# Dataset4j Modular Architecture

## Overview

Dataset4j v1.0.0 introduces a modular architecture that provides both convenience and flexibility for users with different needs.

## Module Structure

### 🏗️ **Parent Project**
```
dataset4j-parent (POM)
├── dataset4j-core      # Core functionality
├── dataset4j-poi       # Excel/CSV support  
├── dataset4j-parquet   # Parquet support
└── dataset4j           # All-in-one convenience module
```

## Usage Options

### **Option A: Complete Library (Recommended for beginners)**
Single dependency with all features:

```xml
<dependency>
    <groupId>io.github.amah</groupId>
    <artifactId>dataset4j</artifactId>
    <version>1.0.0</version>
</dependency>
```

```java
import dataset4j.*;
import dataset4j.poi.*;
import dataset4j.parquet.*;

// All features available
Dataset<Employee> data = DatasetIO.excel()
    .fromFile("employees.xlsx")
    .readAs(Employee.class);

Dataset<Employee> processed = data
    .filter(emp -> emp.department().equals("IT"))
    .sortBy(Employee::salary);

DatasetIO.parquet()
    .toFile("output.parquet")
    .write(processed);
```

### **Option B: Modular Dependencies (Recommended for libraries)**
Pick only the modules you need:

```xml
<!-- Core functionality only -->
<dependency>
    <groupId>io.github.amah</groupId>
    <artifactId>dataset4j-core</artifactId>
    <version>1.0.0</version>
</dependency>

<!-- Add Excel/CSV support -->
<dependency>
    <groupId>io.github.amah</groupId>
    <artifactId>dataset4j-poi</artifactId>
    <version>1.0.0</version>
</dependency>
```

```java
import dataset4j.*;
import dataset4j.poi.*;

// Core + POI functionality
Dataset<Employee> data = ExcelDatasetReader
    .fromFile("employees.xlsx")
    .readAs(Employee.class);

ExcelDatasetWriter
    .toFile("report.xlsx")
    .write(data);
```

## Module Details

### **dataset4j-core**
**Zero external dependencies** (Jakarta Validation is optional)

**Contains:**
- `Dataset<T>` - Core data processing API
- `CompositeKey` - Multi-key support for joins
- `Pair<K,V>` - Key-value data structures
- `@DataColumn` / `@DataTable` - Annotation framework
- `AnnotationProcessor` - Metadata extraction
- `FormatProvider` - Basic formatting utilities

**Use cases:**
- Pure data processing without file I/O
- Library development with minimal dependencies
- Microservices with custom data sources

### **dataset4j-poi** 
**Dependencies:** dataset4j-core, Apache POI, OpenCSV

**Contains:**
- `ExcelDatasetReader` - Read Excel files (.xlsx, .xls)
- `ExcelDatasetWriter` - Write Excel files with formatting
- `CsvDatasetReader` - Read CSV files (planned)
- `CsvDatasetWriter` - Write CSV files (planned)
- Excel-specific annotations for styling

**Use cases:**
- Business reporting and analytics
- Data import/export workflows
- Office document integration

### **dataset4j-parquet**
**Dependencies:** dataset4j-core, Apache Parquet, Hadoop

**Contains:**
- `ParquetDatasetReader` - Read Parquet files
- `ParquetDatasetWriter` - Write Parquet files (planned)
- `ParquetSchemaInfo` - Schema inspection utilities
- Parquet-specific optimizations

**Use cases:**
- Big data analytics
- Data lake integration  
- High-performance columnar storage
- Spark/Hadoop ecosystem integration

### **dataset4j** (Aggregator)
**Dependencies:** All modules above

**Contains:**
- `DatasetIO` - Unified API for all formats
- All functionality from individual modules
- Convenience methods and builders

**Use cases:**
- Rapid prototyping
- Multi-format applications
- When dependency size is not a concern

## Migration from v0.2.0

The complete API remains backward compatible when using the main `dataset4j` artifact:

```xml
<!-- v0.2.0 -->
<dependency>
    <groupId>io.github.amah</groupId>
    <artifactId>dataset4j</artifactId>
    <version>0.2.0</version>
</dependency>

<!-- v1.0.0 - Same usage -->
<dependency>
    <groupId>io.github.amah</groupId>
    <artifactId>dataset4j</artifactId>
    <version>1.0.0</version>
</dependency>
```

**No code changes required** for existing applications.

## Examples

### **Core-Only Usage**
```java
// Lightweight data processing
Dataset<Sale> sales = Dataset.of(salesData)
    .filter(sale -> sale.amount() > 1000)
    .groupBy(Sale::region)
    .aggregate(Sale::amount, Double::sum);
```

### **Excel Integration**
```java
// Read from Excel with annotations
@DataTable(name = "Sales Report")
public record Sale(
    @DataColumn(name = "Date", dateFormat = "MM/dd/yyyy") LocalDate date,
    @DataColumn(name = "Amount", numberFormat = "$#,##0.00") double amount,
    @DataColumn(name = "Region") String region
) {}

Dataset<Sale> sales = ExcelDatasetReader
    .fromFile("sales.xlsx")
    .sheet("Q1 Data")
    .readAs(Sale.class);
```

### **Parquet Analytics** 
```java
// High-performance columnar processing
Dataset<Employee> employees = ParquetDatasetReader
    .fromFile("employees.parquet")
    .readAs(Employee.class);

Dataset<Pair<String, Double>> avgSalaryByDept = employees
    .groupBy(Employee::department)
    .aggregate(Employee::salary, 
        list -> list.stream().mapToDouble(Double::doubleValue).average().orElse(0));
```

### **Multi-Format Workflow**
```java
// Complete ETL pipeline
Dataset<Employee> data = DatasetIO.excel()
    .fromFile("input.xlsx")
    .readAs(Employee.class);

Dataset<Employee> cleaned = data
    .filter(emp -> emp.email() != null)
    .reject(emp -> emp.salary() < 0);

// Output to multiple formats
DatasetIO.excel()
    .toFile("report.xlsx")
    .sheet("Clean Data")
    .write(cleaned);

DatasetIO.parquet()
    .toFile("warehouse/employees.parquet")
    .write(cleaned);
```

## Performance Characteristics

| Module | JAR Size | Dependencies | Use Case |
|--------|----------|--------------|-----------|
| **dataset4j-core** | ~50KB | 0 required | Pure processing |
| **dataset4j-poi** | ~15MB | POI + OpenCSV | Excel/CSV files |
| **dataset4j-parquet** | ~45MB | Parquet + Hadoop | Big data analytics |
| **dataset4j** (all) | ~60MB | All above | Complete toolkit |

## Best Practices

### **For Application Development**
- Use the complete `dataset4j` artifact for rapid development
- Switch to modular approach if JAR size matters for deployment

### **For Library Development**
- Depend only on `dataset4j-core` in your API
- Let users choose format modules based on their needs
- Use `provided` scope for format-specific dependencies

### **For Microservices**
- Use minimal modules to reduce container image size
- Consider `dataset4j-core` only if you don't need file I/O
- Use format-specific modules only when needed

## Future Modules

Planned additional modules:
- **dataset4j-json** - JSON/NDJSON support
- **dataset4j-avro** - Apache Avro support  
- **dataset4j-orc** - Apache ORC support
- **dataset4j-jdbc** - Database connectivity
- **dataset4j-kafka** - Kafka integration

This modular architecture ensures dataset4j can grow while maintaining simplicity for basic use cases.