# Dataset4J - Pandas DataFrame Mapping for AI Assistants

This guide provides comprehensive mapping between pandas DataFrame operations and their Dataset4J equivalents, helping developers and AI assistants translate pandas concepts to type-safe Java code.

## Data Creation and Loading

| Pandas | Dataset4J | Description |
|--------|-----------|-------------|
| `pd.DataFrame(data)` | `Dataset.of(records...)` | Create from data |
| `pd.read_csv("file.csv")` | `CsvDatasetReader.fromFile("file.csv").read(RecordClass.class)` | Load from CSV |
| `pd.read_excel("file.xlsx")` | `ExcelDatasetReader.fromFile("file.xlsx").read(RecordClass.class)` | Load from Excel |
| `pd.read_parquet("file.parquet")` | `ParquetDatasetReader.fromFile("file.parquet").read(RecordClass.class)` | Load from Parquet |

### Example: Data Creation

```python
# Pandas - Dynamic typing
df = pd.DataFrame([
    {'name': 'John', 'age': 30, 'city': 'NYC'},
    {'name': 'Jane', 'age': 25, 'city': 'LA'}
])
```

```java
// Dataset4J - Type-safe with records
@GenerateFields
public record Person(String name, int age, String city) {}

Dataset<Person> dataset = Dataset.of(
    new Person("John", 30, "NYC"),
    new Person("Jane", 25, "LA")
);
```

## Basic Operations

| Pandas | Dataset4J | Description |
|--------|-----------|-------------|
| `df.shape` | `dataset.size()` | Get row count |
| `df.columns` | `metadata.getAllFields().stream().map(FieldMeta::getFieldName)` | Get column names |
| `df.head(n)` | `dataset.limit(n)` | Get first n rows |
| `df.tail(n)` | `dataset.takeLast(n)` | Get last n rows |
| `df.info()` | `metadata.getAllFields()` | Get column info |
| `df.describe()` | Custom aggregation | Statistical summary |

### Example: Basic Information

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

## Filtering and Selection

| Pandas | Dataset4J | Description |
|--------|-----------|-------------|
| `df[df['age'] > 25]` | `dataset.filter(p -> p.age() > 25)` | Filter rows |
| `df[df['city'].isin(['NYC', 'LA'])]` | `dataset.filter(p -> Set.of("NYC", "LA").contains(p.city()))` | Filter by values |
| `df[['name', 'age']]` | Projection to new record type | Select columns |
| `df.loc[df['age'] > 25, ['name']]` | Combined filter and projection | Filter and select |

### Example: Filtering

```python
# Pandas - String-based column access
adults = df[df['age'] > 25]
names_and_ages = df[['name', 'age']]
filtered_names = df.loc[df['age'] > 25, ['name']]
```

```java
// Dataset4J - Type-safe field access
Dataset<Person> adults = dataset.filter(p -> p.age() > 25);

// Column selection requires projection to new record type
public record NameAge(String name, int age) {}
Dataset<NameAge> namesAndAges = dataset.map(p -> new NameAge(p.name(), p.age()));

// Combined filter and project
Dataset<String> filteredNames = dataset
    .filter(p -> p.age() > 25)
    .map(Person::name);
```

## Column Operations

| Pandas | Dataset4J | Description |
|--------|-----------|-------------|
| `df['new_col'] = df['age'] * 2` | Map to new record type with additional field | Add column |
| `df.drop(['col'], axis=1)` | Field selection without dropped column | Drop columns |
| `df.rename(columns={'old': 'new'})` | Create new record type with renamed field | Rename columns |
| `df['age'].apply(lambda x: x * 2)` | `dataset.map(p -> p.age() * 2)` | Transform column |

### Example: Adding Columns

```python
# Pandas - Dynamic column addition
df['age_doubled'] = df['age'] * 2
df_renamed = df.rename(columns={'age': 'years'})
```

```java
// Dataset4J - Requires new record types for schema changes
public record PersonWithDouble(String name, int age, String city, int ageDoubled) {}
Dataset<PersonWithDouble> withDoubled = dataset
    .map(p -> new PersonWithDouble(p.name(), p.age(), p.city(), p.age() * 2));

// Renaming requires new record type
public record PersonRenamed(String name, int years, String city) {}
Dataset<PersonRenamed> renamed = dataset
    .map(p -> new PersonRenamed(p.name(), p.age(), p.city()));
```

## Grouping and Aggregation

| Pandas | Dataset4J | Description |
|--------|-----------|-------------|
| `df.groupby('city')` | `dataset.groupBy(Person::city)` | Group by column |
| `df.groupby('city').size()` | `dataset.groupBy(Person::city).map(g -> g.size())` | Count by group |
| `df.groupby('city')['age'].mean()` | Custom aggregation with streams | Mean by group |
| `df.groupby('city').agg({'age': ['mean', 'count']})` | Multiple aggregations | Complex aggregation |

### Example: Grouping

```python
# Pandas - Built-in aggregation functions
city_counts = df.groupby('city').size()
avg_age_by_city = df.groupby('city')['age'].mean()
```

```java
// Dataset4J - Explicit aggregation with new record types
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

## Joining Operations

| Pandas | Dataset4J | Description |
|--------|-----------|-------------|
| `df1.merge(df2, on='key')` | `dataset1.innerJoin(dataset2, keyExtractor1, keyExtractor2)` | Inner join |
| `df1.merge(df2, on='key', how='left')` | `dataset1.leftJoin(dataset2, keyExtractor1, keyExtractor2)` | Left join |
| `df1.merge(df2, on='key', how='right')` | `dataset1.rightJoin(dataset2, keyExtractor1, keyExtractor2)` | Right join |
| `df1.merge(df2, on='key', how='outer')` | `dataset1.fullOuterJoin(dataset2, keyExtractor1, keyExtractor2)` | Outer join |

### Example: Joining

```python
# Pandas - String-based join keys
df_employees = pd.DataFrame([{'emp_id': 1, 'name': 'John'}, {'emp_id': 2, 'name': 'Jane'}])
df_salaries = pd.DataFrame([{'emp_id': 1, 'salary': 50000}, {'emp_id': 2, 'salary': 60000}])
joined = df_employees.merge(df_salaries, on='emp_id')
```

```java
// Dataset4J - Type-safe join with function references
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

## Sorting Operations

| Pandas | Dataset4J | Description |
|--------|-----------|-------------|
| `df.sort_values('age')` | `dataset.sorted(Comparator.comparing(Person::age))` | Sort by column |
| `df.sort_values('age', ascending=False)` | `dataset.sorted(Comparator.comparing(Person::age).reversed())` | Sort descending |
| `df.sort_values(['city', 'age'])` | `dataset.sorted(Comparator.comparing(Person::city).thenComparing(Person::age))` | Multi-column sort |

### Example: Sorting

```python
# Pandas - String-based column references
sorted_by_age = df.sort_values('age')
sorted_desc = df.sort_values('age', ascending=False)
sorted_multi = df.sort_values(['city', 'age'])
```

```java
// Dataset4J - Type-safe method references
Dataset<Person> sortedByAge = dataset
    .sorted(Comparator.comparing(Person::age));

Dataset<Person> sortedDesc = dataset
    .sorted(Comparator.comparing(Person::age).reversed());

Dataset<Person> sortedMulti = dataset
    .sorted(Comparator.comparing(Person::city)
           .thenComparing(Person::age));
```

## Statistical Operations

| Pandas | Dataset4J | Description |
|--------|-----------|-------------|
| `df['age'].sum()` | `dataset.stream().mapToInt(Person::age).sum()` | Sum column |
| `df['age'].mean()` | `dataset.stream().mapToInt(Person::age).average().orElse(0)` | Mean column |
| `df['age'].max()` | `dataset.stream().mapToInt(Person::age).max().orElse(0)` | Max column |
| `df['age'].min()` | `dataset.stream().mapToInt(Person::age).min().orElse(0)` | Min column |
| `df['age'].count()` | `dataset.size()` | Count rows |

### Example: Statistics

```python
# Pandas - Built-in statistical functions
age_stats = {
    'sum': df['age'].sum(),
    'mean': df['age'].mean(),
    'max': df['age'].max(),
    'min': df['age'].min()
}
```

```java
// Dataset4J - Stream-based calculations with explicit types
public record AgeStats(int sum, double mean, int max, int min) {}
AgeStats ageStats = new AgeStats(
    dataset.stream().mapToInt(Person::age).sum(),
    dataset.stream().mapToInt(Person::age).average().orElse(0),
    dataset.stream().mapToInt(Person::age).max().orElse(0),
    dataset.stream().mapToInt(Person::age).min().orElse(0)
);
```

## Data Export

| Pandas | Dataset4J | Description |
|--------|-----------|-------------|
| `df.to_csv('file.csv')` | `CsvDatasetWriter.toFile('file.csv').write(dataset)` | Export to CSV |
| `df.to_excel('file.xlsx')` | `ExcelDatasetWriter.toFile('file.xlsx').write(dataset)` | Export to Excel |
| `df.to_parquet('file.parquet')` | `ParquetDatasetWriter.toFile('file.parquet').write(dataset)` | Export to Parquet |

### Example: Export with Field Selection

```python
# Pandas - Column selection with strings
df.to_excel('report.xlsx', index=False)
df[['name', 'age']].to_csv('names_ages.csv')
```

```java
// Dataset4J - Type-safe field selection with generated constants
ExcelDatasetWriter
    .toFile("report.xlsx")
    .headers(true)
    .write(dataset);

// Field selection with generated constants (preferred)
ExcelDatasetWriter
    .toFile("names_ages.xlsx")
    .fields(Person.Fields.NAME, Person.Fields.AGE)
    .write(dataset);

// Bulk selection with generated arrays
ExcelDatasetWriter
    .toFile("all_fields.xlsx")
    .fieldsArray(Person.Fields.ALL_FIELDS)
    .write(dataset);
```

## Key Differences and Design Philosophy

| Aspect | Pandas | Dataset4J |
|--------|--------|-----------|
| **Type Safety** | Dynamic typing, runtime errors | Compile-time type safety |
| **Mutability** | Mutable DataFrames | Immutable Dataset operations |
| **Schema** | Dynamic column addition/removal | Fixed schema via record types |
| **Column Selection** | String-based column names | Generated type-safe constants |
| **Performance** | NumPy-backed, optimized for bulk ops | JVM optimization, streams |
| **Memory Model** | In-memory with copy-on-write | Immutable with structural sharing |
| **Error Handling** | Runtime exceptions | Compile-time type checking |

## Migration Strategy

### 1. Schema-First Approach

```python
# Pandas - Schema emerges from data
df = pd.read_csv('data.csv')  # Infers types
df['total'] = df['price'] * df['quantity']  # Adds column dynamically
```

```java
// Dataset4J - Define schema explicitly
@GenerateFields
public record Product(String name, double price, int quantity) {}
public record ProductWithTotal(String name, double price, int quantity, double total) {}

Dataset<Product> products = CsvDatasetReader
    .fromFile("data.csv")
    .read(Product.class);
    
Dataset<ProductWithTotal> withTotal = products
    .map(p -> new ProductWithTotal(p.name(), p.price(), p.quantity(), p.price() * p.quantity()));
```

### 2. Immutable Operations

```python
# Pandas - Mutable operations
df.dropna(inplace=True)      # Modifies existing DataFrame
df.sort_values('age', inplace=True)  # Modifies existing DataFrame
```

```java
// Dataset4J - Immutable chain
Dataset<Person> cleaned = dataset
    .filter(p -> p.age() != null)    // Returns new Dataset
    .sorted(Comparator.comparing(Person::age));  // Returns new Dataset
```

### 3. Type-Safe Field Access

```python
# Pandas - String-based (error-prone)
df['emial']  # Typo causes runtime error
```

```java
// Dataset4J - Compile-time safety
dataset.map(Person::email);  // Typo caught by compiler
// Or with generated constants:
.fields(Person.Fields.EMAIL)  // IDE auto-completion prevents typos
```

## Performance Considerations

### Pandas Strengths
- **Vectorized operations** on large datasets
- **Memory-efficient** with NumPy backend
- **Mature optimization** for numerical computing

### Dataset4J Strengths  
- **JVM optimization** and garbage collection
- **No data copying** with immutable operations
- **Type safety** eliminates runtime type errors
- **Parallel processing** with Java Streams

## When to Use Each

### Use Pandas When:
- Working with numerical/scientific data
- Need maximum performance for large datasets
- Rapid prototyping and data exploration
- Team has strong Python expertise

### Use Dataset4J When:
- Building production Java applications
- Type safety and compile-time validation required
- Integration with existing Java ecosystem
- Long-term maintainability is priority

## Migration Example: Complete Workflow

```python
# Pandas - Dynamic, mutable workflow
import pandas as pd

df = pd.read_excel('sales.xlsx')
df['total'] = df['price'] * df['quantity']  
high_value = df[df['total'] > 1000][['customer', 'total']]
result = high_value.sort_values('total', ascending=False)
result.to_excel('high_value_customers.xlsx')
```

```java
// Dataset4J - Type-safe, immutable workflow
@GenerateFields
public record Sale(String customer, double price, int quantity) {}
public record SaleWithTotal(String customer, double price, int quantity, double total) {}
public record CustomerTotal(String customer, double total) {}

Dataset<Sale> sales = ExcelDatasetReader
    .fromFile("sales.xlsx")
    .read(Sale.class);

Dataset<CustomerTotal> result = sales
    .map(s -> new SaleWithTotal(s.customer(), s.price(), s.quantity(), s.price() * s.quantity()))
    .filter(s -> s.total() > 1000)
    .map(s -> new CustomerTotal(s.customer(), s.total()))
    .sorted(Comparator.comparing(CustomerTotal::total).reversed());

ExcelDatasetWriter
    .toFile("high_value_customers.xlsx")
    .fields(CustomerTotal.Fields.CUSTOMER, CustomerTotal.Fields.TOTAL)
    .write(result);
```

This mapping provides AI assistants with comprehensive guidance for translating pandas operations to Dataset4J while emphasizing the benefits of type safety, immutability, and compile-time validation.