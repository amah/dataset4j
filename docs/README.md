# Dataset4J Documentation

This directory contains comprehensive documentation for the Dataset4J library.

## 📁 **Structure**

### 📊 **Core Features**

- **[`TABLE.md`](TABLE.md)** - `Table` — untyped tabular data container (schema-free sibling of `Dataset<T>`)
- **[`CELLVALUE_COERCION.md`](CELLVALUE_COERCION.md)** - CellValue coercion rules and source-specific type mapping

### 🤖 **AI Instructions** (`ai-instructions/`)
Complete guidance for AI coding assistants working with Dataset4J:

- **`dataset4j-ai-instructions-overview.md`** - Library overview and quick start
- **`dataset4j-ai-instructions-core-concepts.md`** - Core concepts and principles  
- **`dataset4j-ai-instructions-usage-patterns.md`** - Common usage patterns
- **`dataset4j-ai-instructions-pandas-mapping.md`** - Pandas DataFrame mapping
- **`dataset4j-ai-instructions-guidelines.md`** - Code generation best practices
- **`dataset4j-ai-coding-instructions.md`** - Comprehensive combined guide

### 🏗️ **Architecture** (`architecture/`)
Detailed technical architecture documentation:

- **`MODULAR_ARCHITECTURE.md`** - Module structure and usage options
- **`PARQUET_SECURITY.md`** - Security-safe Parquet implementation  
- **`FIELD_CONSTANTS_GENERATION.md`** - Compile-time field constants

## 🎯 **Usage**

### For AI Assistants
Start with `ai-instructions/dataset4j-ai-instructions-overview.md` for a complete introduction to the library's capabilities and patterns.

### For Developers
Review `architecture/MODULAR_ARCHITECTURE.md` to understand the library's structure and choose the appropriate modules for your needs.

### For Contributors
The `architecture/` documents provide deep technical insights into the library's design decisions and implementation details.

## 📚 **Additional Resources**

- **Main README**: `../README.md` - Project overview and getting started
- **Examples**: Source code examples in each module's `src/main/java/*/examples/`
- **Tests**: Unit tests demonstrating usage patterns
- **Maven Central**: https://central.sonatype.com/artifact/io.github.amah/dataset4j