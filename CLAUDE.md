# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Build and Test
```bash
# Run all tests
mvn clean test

# Compile only
mvn compile

# Clean build artifacts
mvn clean

# Generate javadocs
mvn javadoc:javadoc

# Package jar
mvn package
```

### Running Single Tests
```bash
# Run a specific test class
mvn test -Dtest=DatasetTest

# Run a specific test method
mvn test -Dtest=DatasetTest#testMethodName
```

## Architecture Overview

### Core Components

**dataset4j** is a minimal Java library with three main classes in the `dataset4j` package:

1. **Dataset&lt;T&gt;** (`src/main/java/dataset4j/Dataset.java`) - The main DataFrame-like class that wraps `List<T>` with Pandas-inspired operations. Key characteristics:
   - Immutable - all operations return new instances
   - Fluent API supporting method chaining
   - Generic type T typically represents record classes
   - Supports filtering, sorting, grouping, joining, and aggregation operations
   - Includes multi-key join support via CompositeKey

2. **Pair&lt;L, R&gt;** (`src/main/java/dataset4j/Pair.java`) - Generic container for join results:
   - Used as default return type for two-table joins
   - Provides `.left()` and `.right()` accessors
   - Supports transformation methods like `mapLeft()`, `mapRight()`
   - Positional aliases: `.first()` and `.second()`

3. **CompositeKey** (`src/main/java/dataset4j/CompositeKey.java`) - Multi-key support for joins:
   - Combines multiple fields into a single key for joining
   - Used internally by multi-key join methods
   - Factory methods: `CompositeKey.of()` and static `key()` for imports
   - Proper equals/hashCode implementation for join performance

### Design Philosophy

- **Zero dependencies** - only JDK 17+ required (except JUnit for tests)
- **Records-first** - designed to work with Java records as row types
- **Stream-compatible** - integrates with Java Streams API
- **Pandas-inspired** - method names and patterns mirror Pandas operations

### Test Structure

Tests are in `src/test/java/dataset4j/DatasetTest.java` using JUnit Jupiter. The test class uses sample records:
- `Employee(String name, int age, String dept)`  
- `Department(String dept, String location)`
- `Budget(String dept, int amount)`

The library has been simplified to remove the Triplet concept - complex multi-table joins can be achieved by chaining multiple two-table joins returning Pairs.

### Multi-Key Joins

The library supports joining on multiple keys using the `CompositeKey` class:

```java
import static dataset4j.CompositeKey.key;
import static dataset4j.CompositeKey.on;

// Method 1: Fluent API with property accessors (recommended)
employees.innerJoinOn(departments,
    on(Employee::dept, Employee::location),
    on(Department::dept, Department::location));

// Method 2: Using CompositeKey with static import
employees.innerJoinMulti(departments,
    e -> key(e.dept(), e.location()),
    d -> key(d.dept(), d.location()));

// Method 3: Convenience methods for 2 or 3 keys
employees.innerJoin2(departments,
    Employee::dept, Employee::location,
    Department::dept, Department::location);
```

Available multi-key methods: `innerJoinOn()`, `leftJoinOn()`, `rightJoinOn()`, `innerJoinMulti()`, `leftJoinMulti()`, `rightJoinMulti()`, `innerJoin2()`, `leftJoin2()`, `innerJoin3Keys()`.

## Requirements

- Java 17+ (uses records and modern language features)
- Maven 3.x for build management
- JUnit 5.11.4 for testing