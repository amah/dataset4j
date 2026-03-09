# Dataset4J - AI Coding Assistant Instructions

## 🚀 **Quick Introduction**

Dataset4J is a modular Java library that brings **pandas-like DataFrame operations** to Java records with strong typing, annotation-driven metadata, and comprehensive format support (Excel, Parquet, CSV).

### **Core Philosophy**
- **Type Safety**: Java records + compile-time validation
- **Immutability**: All operations return new Dataset instances  
- **Fluent API**: Chainable operations like pandas
- **Security First**: Built-in path validation and safe dependencies

### **Quick Example**
```java
@GenerateFields(className = "Fields", columnsClassName = "Cols")
public record Employee(
    @DataColumn(name = "Employee ID", order = 1, required = true) String id,
    @DataColumn(name = "Full Name", order = 2) String fullName,
    @DataColumn(name = "Email", order = 3) String email
) {}

// Pandas-like operations with type safety
Dataset<Employee> engineers = employees
    .filter(emp -> emp.department().equals("Engineering"))
    .sortBy(Employee::fullName);

// Type-safe Excel export  
ExcelDatasetWriter
    .toFile("report.xlsx")
    .fields(Employee.Fields.ID, Employee.Fields.FULL_NAME, Employee.Fields.EMAIL)
    .write(engineers);
```

## 📊 **DataFrame → Dataset Mapping**

| **Pandas DataFrame** | **Dataset4J** | **Key Difference** |
|---------------------|---------------|-------------------|
| `pd.DataFrame(data)` | `Dataset.of(records...)` | Type-safe records vs dynamic dicts |
| `df.filter(condition)` | `dataset.filter(predicate)` | Compile-time type checking |
| `df['column']` | `dataset.map(Record::field)` | Method references vs strings |
| `df.groupby('col')` | `dataset.groupBy(Record::field)` | Type-safe field access |
| `df.sort_values('col')` | `dataset.sortBy(Record::field)` | No magic strings |
| `df[['col1', 'col2']]` | Field selection with generated constants | Compile-time field validation |
| `df.to_excel('file.xlsx')` | `ExcelDatasetWriter.toFile().write()` | Builder pattern with validation |
| `pd.read_excel('file.xlsx')` | `ExcelDatasetReader.fromFile().read(Class)` | Schema-first approach |

## 📚 **Complete Documentation**

This document serves as an **index** to focused instruction files. Each file covers specific aspects in detail:

### **🎯 Getting Started**
📄 **[Overview](dataset4j-ai-instructions-overview.md)**
- Library architecture and module structure  
- Quick start guide for AI assistants
- Version compatibility and dependencies

### **🧠 Core Concepts** 
📄 **[Core Concepts](dataset4j-ai-instructions-core-concepts.md)**
- Dataset<T> immutable DataFrame principles
- Record-based data model with annotations
- Compile-time field constants generation  
- Runtime metadata system and caching

### **🔧 Common Patterns**
📄 **[Usage Patterns](dataset4j-ai-instructions-usage-patterns.md)**
- Basic Dataset operations (filter, map, group)
- Field selection with generated constants
- Excel and Parquet I/O operations
- Validation and error handling patterns

### **🐼 Pandas Migration**
📄 **[Pandas Mapping](dataset4j-ai-instructions-pandas-mapping.md)**
- Comprehensive pandas DataFrame to Dataset4J mapping
- Side-by-side Python/Java examples
- Migration strategies and design philosophy
- Complete operations translation guide

### **✨ Best Practices**
📄 **[Guidelines](dataset4j-ai-instructions-guidelines.md)**
- Code generation best practices
- Type safety and annotation patterns
- Error handling and performance tips
- Security guidelines and testing patterns

## 🎯 **Quick Reference**

### **Modular Dependencies**
```xml
<!-- Core only (50KB, zero dependencies) -->
<dependency>
    <groupId>io.github.amah</groupId>
    <artifactId>dataset4j-core</artifactId>
    <version>1.0.0</version>
</dependency>

<!-- Complete library (all features) -->
<dependency>
    <groupId>io.github.amah</groupId>
    <artifactId>dataset4j</artifactId>
    <version>1.0.0</version>
</dependency>
```

### **Generated Constants**
```java
// Auto-generated at compile time
Employee.Fields.ID          // "id" 
Employee.Fields.FULL_NAME   // "fullName"
Employee.Cols.COL_ID        // "Employee ID"  
Employee.Cols.COL_FULL_NAME // "Full Name"
```

### **Common Operations**
```java
// Filtering and transformation
Dataset<String> emails = employees
    .filter(emp -> emp.salary() > 50000)
    .map(Employee::email);

// Grouping and aggregation  
Dataset<DeptStats> stats = employees
    .groupBy(Employee::department)
    .map(group -> new DeptStats(group.getKey(), group.getData().size()));

// Type-safe field selection
FieldSelector.from(metadata)
    .fields(Employee.Fields.ID, Employee.Fields.FULL_NAME)
    .requiredOnly()
    .select();
```

---

**For detailed guidance on any specific aspect, please refer to the focused documentation files above.**