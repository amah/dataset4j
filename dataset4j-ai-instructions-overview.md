# Dataset4J - AI Coding Assistant Instructions Overview

## What is Dataset4J?

Dataset4J is a Java library providing a pandas-like DataFrame API for Java records, with strong typing, annotation-driven metadata, and support for Excel/Parquet I/O operations. This set of instruction files provides comprehensive guidance for AI coding assistants working with this library.

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

### Security Features

The library includes built-in security measures:
- ✅ Path traversal protection on all file operations
- ✅ File size limits to prevent DoS attacks
- ✅ Thread-safe operations throughout
- ✅ Input validation on format strings

## Instruction Files Structure

This documentation is split into focused sections for better usability:

### 1. Core Concepts
📄 **`dataset4j-ai-instructions-core-concepts.md`**
- Dataset<T> immutable DataFrame
- Record-based data model with annotations
- Compile-time field constants generation
- Runtime metadata system and caching

### 2. Common Usage Patterns  
📄 **`dataset4j-ai-instructions-usage-patterns.md`**
- Basic Dataset operations (filter, map, group)
- Field selection with generated constants
- Excel and Parquet I/O operations
- Practical examples and workflows

### 3. Pandas Migration Guide
📄 **`dataset4j-ai-instructions-pandas-mapping.md`**
- Comprehensive pandas DataFrame to Dataset4J mapping
- Side-by-side Python/Java examples
- Migration tips and design philosophy differences
- Common operations translation guide

### 4. AI Assistant Guidelines
📄 **`dataset4j-ai-instructions-guidelines.md`**
- Code generation best practices
- Annotation usage patterns
- Error handling and type safety
- Performance considerations

### 5. Anti-Patterns and Troubleshooting
📄 **`dataset4j-ai-instructions-troubleshooting.md`**
- Common mistakes to avoid
- Debugging field selection issues
- Performance optimization tips
- Security best practices

## Quick Start for AI Assistants

When generating Dataset4J code, always:

1. **Use Java Records** for data structures
2. **Use Generated Constants** instead of magic strings  
3. **Follow Immutable Patterns** - operations return new Dataset instances
4. **Leverage Type Safety** - let compiler catch errors
5. **Trust Built-in Security** - file operations are automatically protected

## Example: Basic Usage

```java
// Define record with annotations
@GenerateFields(className = "Fields", columnsClassName = "Cols")
public record Employee(
    @DataColumn(name = "Employee ID", order = 1, required = true)
    String id,
    @DataColumn(name = "Name", order = 2) 
    String name,
    @DataColumn(name = "Email", order = 3)
    String email
) {}

// Create and manipulate dataset
Dataset<Employee> employees = Dataset.of(
    new Employee("001", "John", "john@example.com"),
    new Employee("002", "Jane", "jane@example.com")
);

// Type-safe operations with generated constants
Dataset<String> emails = employees
    .filter(emp -> emp.name().startsWith("J"))
    .map(Employee::email);

// Excel export with field selection
ExcelDatasetWriter
    .toFile("report.xlsx")
    .fields(Employee.Fields.ID, Employee.Fields.NAME, Employee.Fields.EMAIL)
    .write(employees);
```

## Version Compatibility

- **Java Version**: Requires Java 17+ (uses Records, Pattern Matching)
- **Maven**: Compatible with Maven 3.6+
- **Dependencies**: All managed through Maven, no manual classpath setup needed

## Getting Started

1. Read the **Core Concepts** file to understand fundamental principles
2. Review **Usage Patterns** for common operations
3. Check **Pandas Mapping** if migrating from pandas experience
4. Follow **Guidelines** for best practices
5. Reference **Troubleshooting** when issues arise

Each instruction file is self-contained but cross-references others where relevant. This modular approach allows AI assistants to quickly find specific information without parsing through a large monolithic document.