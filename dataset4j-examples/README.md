# Dataset4J Examples

This module contains comprehensive examples and demonstrations for the Dataset4J library. **This module is not deployed to Maven Central** - it's only for development and documentation purposes.

## Available Examples

### 1. QuickStartExample
- **Purpose**: Basic introduction to Dataset4J
- **Features**: Creating datasets, basic transformations, Excel/Parquet export
- **Best for**: First-time users learning the basics

### 2. ComprehensiveExample  
- **Purpose**: Showcase all major features across formats
- **Features**: Excel, Parquet, transformations, grouping, statistics
- **Best for**: Understanding the full capabilities

### 3. PerformanceExample
- **Purpose**: Performance benchmarking and compression analysis
- **Features**: Large datasets, timing, compression ratios, throughput
- **Best for**: Performance evaluation and format comparison

### 4. Legacy Examples (from core)
- **CompileTimeFieldsUsage**: Compile-time field access patterns
- **FieldSelectionExample**: Dynamic field selection
- **GeneratedFieldsExample**: Annotation processor usage

## Running Examples

### Command Line
```bash
# Run specific example
mvn exec:java -pl dataset4j-examples -Dexec.mainClass="dataset4j.examples.QuickStartExample"
mvn exec:java -pl dataset4j-examples -Dexec.mainClass="dataset4j.examples.ComprehensiveExample"
mvn exec:java -pl dataset4j-examples -Dexec.mainClass="dataset4j.examples.PerformanceExample"
```

### IDE
Simply run the `main` method of any example class in your IDE.

## Example Output

### QuickStartExample
```
=== Dataset4J Quick Start ===

Created dataset with 3 people
High earners (>$70K): 2
  Jane Smith: $82000
  John Doe: $75000

Exporting to: /tmp/dataset4j-quickstart8234567890
✓ Excel file created
✓ Parquet file created

File sizes:
  Excel:   8,432 bytes
  Parquet: 1,234 bytes
```

### Performance Results
Typical performance on modern hardware:
- **Excel**: ~1,000-5,000 records/sec
- **Parquet SNAPPY**: ~10,000-50,000 records/sec  
- **Parquet GZIP**: ~5,000-25,000 records/sec (best compression)
- **Parquet LZ4**: ~15,000-75,000 records/sec (fastest)

## Key Features Demonstrated

### Data Types
- ✅ Integers, Strings, Booleans
- ✅ BigDecimal for precise financial calculations
- ✅ LocalDate for date handling
- ✅ Custom formatting (currency, dates)

### File Formats
- ✅ **Excel (.xlsx)**: Human-readable, good for reports
- ✅ **Parquet**: Columnar, excellent compression, fast analytics

### Compression (Parquet)
- ✅ **SNAPPY**: Fast, balanced compression
- ✅ **LZ4**: Fastest, good compression
- ✅ **GZIP**: Best compression ratio
- ✅ **UNCOMPRESSED**: Fastest read/write

### Data Operations
- ✅ Filtering, sorting, grouping
- ✅ Mapping and transformations
- ✅ Aggregations (count, sum, average)
- ✅ Top-N queries

## Module Configuration

This module is configured to:
- ✅ Skip Maven deployment (`maven.deploy.skip=true`)
- ✅ Skip installation (`maven.install.skip=true`)  
- ✅ Skip source/javadoc generation
- ✅ Skip GPG signing in release profile
- ✅ Skip central publishing

This ensures examples are never accidentally published to Maven Central.