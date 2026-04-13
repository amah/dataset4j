# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Dataset4J is a modular, lightweight DataFrame-like library for Java records. It provides type-safe, immutable Dataset operations with fluent APIs for Excel, CSV, and Parquet I/O.

## Build & Test Commands

```bash
mvn clean test                              # Run all tests
mvn test -pl dataset4j-core                 # Run tests for a specific module
mvn test -Dtest=ExcelSimpleTest             # Run a single test class
mvn test -Dtest="ExcelSimpleTest#testName"  # Run a single test method
mvn clean compile                           # Compile all modules
mvn clean package -DskipTests               # Build JARs without tests
```

Java 17+ required. Maven 3.6+.

## Module Structure

- **dataset4j-core** — Core `Dataset<T>` API, `Table` (untyped tabular data), `CellValue`/`ValueType` (source type preservation), annotation framework (`@DataColumn`, `@GenerateFields`, `@DataTable`), metadata extraction (`PojoMetadata`, `FieldMeta`, `FieldSelector`), and the `FieldConstantProcessor` annotation processor. No external dependencies.
- **dataset4j-poi** — Excel read/write (Apache POI), CSV read/write (OpenCSV). Key classes: `ExcelDatasetReader`, `ExcelDatasetWriter`, `CsvDatasetReader`, `CsvDatasetWriter`, `CellWriter` interface, `DefaultCellWriter`. Depends on dataset4j-core.
- **dataset4j-parquet** — Lightweight Parquet read/write without Hadoop. Key classes: `ParquetDatasetReader`, `ParquetDatasetWriter`. Depends on dataset4j-core.
- **dataset4j** — Shaded uber-JAR aggregating all modules for single-dependency consumption.

## Architecture

**Dataset<T>** is the central abstraction — an immutable, generic wrapper around `List<T>` that works with Java records. All operations (filter, map, groupBy, sortBy, concat, etc.) return new Dataset instances.

**Annotation-driven metadata**: Record components are annotated with `@DataColumn` (column name, order, formatting, cell type, etc.). `@GenerateFields` triggers an annotation processor (`FieldConstantProcessor`) that generates static field-name constants at compile time for type-safe field selection.

**Metadata layer**: `PojoMetadata<T>` (cached via `MetadataCache`) holds all field metadata for a record class. `FieldMeta` represents a single field. `FieldSelector<T>` provides a fluent API for choosing/ordering fields for export.

**Reader/Writer pattern**: All I/O classes use fluent builders (e.g., `ExcelDatasetWriter.toFile("out.xlsx").sheet("Data").fields(...).write(dataset)`). Writers accept a `Dataset<T>` and use metadata reflection to map record components to columns. The `CellWriter` functional interface allows custom cell formatting; `DefaultCellWriter` handles standard type mapping.

**Shared rendering**: `ExcelSheetRenderer` + `SheetRenderConfig` (package-private) contain the shared Excel sheet rendering logic used by both `ExcelDatasetWriter` (single-sheet) and `ExcelWorkbookWriter` (multi-sheet).

**Table (untyped data)**: `Table` is the schema-free sibling of `Dataset<T>`. It stores rows as `List<Map<String, CellValue>>` where each `CellValue` carries the raw value, a `ValueType` enum (STRING, NUMBER, BOOLEAN, DATE, DATETIME, TIME, FORMULA, BLANK, ERROR), and an optional format string from the source (e.g. Excel `"$#,##0.00"`). Typed column accessors (`intColumn()`, `doubleColumn()`, etc.) use coercion, not casting. Readers have `readTable()` methods; writers have `writeTable()` methods. `Dataset.toTable()` and `Table.toDataset(Class<T>)` convert between the two. See `docs/TABLE.md` for full documentation.
