# Dataset4J - Pandas DataFrame Mapping for AI Assistants

This guide provides comprehensive mapping between pandas DataFrame operations and their Dataset4J equivalents, helping developers and AI assistants translate pandas concepts to type-safe Java code.

---

## Table of Contents

- [Equivalence Summary](#equivalence-summary)
- [Data Creation and Loading](#data-creation-and-loading)
- [Basic Operations](#basic-operations)
- [Filtering and Selection](#filtering-and-selection)
- [Slicing](#slicing)
- [Column Operations](#column-operations)
- [Sorting Operations](#sorting-operations)
- [Transformation](#transformation)
- [Deduplication](#deduplication)
- [Grouping and Aggregation](#grouping-and-aggregation)
- [Joining Operations](#joining-operations)
- [Statistical Operations](#statistical-operations)
- [Window and Stateful Operations](#window-and-stateful-operations)
- [String Operations](#string-operations)
- [Reshaping](#reshaping)
- [Data Export and Display](#data-export-and-display)
- [Key Differences and Design Philosophy](#key-differences-and-design-philosophy)
- [Migration Strategy](#migration-strategy)
- [Migration Example: Complete Workflow](#migration-example-complete-workflow)
- [Performance Considerations](#performance-considerations)
- [When to Use Each](#when-to-use-each)

---

## Equivalence Summary

**Legend:** **Available** — Method exists in Dataset4j | **Partial** — Similar functionality via lambdas/map | **Not Available** — No equivalent yet

| Category | Available | Partial | Not Available | Total |
|----------|:---------:|:-------:|:-------------:|:-----:|
| Construction & I/O | 10 | 0 | 4 | 14 |
| Basic Properties | 8 | 2 | 4 | 14 |
| Filtering & Selection | 4 | 3 | 2 | 9 |
| Slicing | 4 | 0 | 0 | 4 |
| Sorting | 3 | 0 | 2 | 5 |
| Transformation | 3 | 3 | 4 | 10 |
| Deduplication | 4 | 0 | 1 | 5 |
| Aggregation | 13 | 1 | 5 | 19 |
| Column Extraction | 4 | 2 | 1 | 7 |
| GroupBy | 12 | 1 | 3 | 16 |
| Joins / Merge | 10 | 1 | 2 | 13 |
| Window / Stateful | 4 | 0 | 5 | 9 |
| String Operations | 0 | 5 | 1 | 6 |
| Reshaping | 0 | 1 | 4 | 5 |
| Display & Output | 2 | 1 | 3 | 6 |
| **Total** | **81** | **20** | **41** | **142** |

Dataset4j covers the most commonly used pandas operations for data processing pipelines. String and reshaping operations are handled through Java's native String methods and custom `map`/`flatMap` lambdas rather than dedicated API methods.

---

## Data Creation and Loading

| Pandas | Dataset4J | Status |
|--------|-----------|--------|
| `pd.DataFrame(data)` | `Dataset.of(list)` / `Dataset.of(rows...)` | Available |
| `pd.DataFrame()` (empty) | `Dataset.empty()` | Available |
| `pd.concat([df1, df2])` | `Dataset.concat(ds1, ds2)` / `ds1.concat(ds2)` | Available |
| `pd.read_csv()` | `CsvDatasetReader.fromFile("file.csv").read(RecordClass.class)` | Available |
| `pd.read_excel()` | `ExcelDatasetReader.fromFile("file.xlsx").read(RecordClass.class)` | Available |
| `pd.read_parquet()` | `ParquetDatasetReader.fromFile("file.parquet").read(RecordClass.class)` | Available |
| `df.to_csv()` | `CsvDatasetWriter.toFile("file.csv").write(dataset)` | Available |
| `df.to_excel()` | `ExcelDatasetWriter.toFile("file.xlsx").write(dataset)` | Available |
| `df.to_parquet()` | `ParquetDatasetWriter.toFile("file.parquet").write(dataset)` | Available |
| `df.copy()` | `ds.copy()` | Available |
| `pd.read_json()` | — | Not Available |
| `pd.read_sql()` | — | Not Available |
| `df.to_json()` | — | Not Available |
| `df.to_sql()` | — | Not Available |

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

---

## Basic Operations

| Pandas | Dataset4J | Status |
|--------|-----------|--------|
| `len(df)` | `ds.size()` | Available |
| `df.empty` | `ds.isEmpty()` | Available |
| `df.iloc[i]` | `ds.get(i)` | Available |
| `df.iloc[0]` | `ds.first()` | Available |
| `df.iloc[-1]` | `ds.last()` | Available |
| `df.values.tolist()` | `ds.toList()` | Available |
| `df.iterrows()` | `ds.iterator()` / `ds.forEachIndexed()` | Available |
| `df.stream()` (no direct pandas equiv) | `ds.stream()` | Available |
| `df.shape` | `ds.size()` (rows only) | Partial |
| `df.columns` | `metadata.getAllFields().stream().map(FieldMeta::getFieldName)` | Partial |
| `df.dtypes` | — (types are compile-time via generics) | Not Available |
| `df.info()` | — | Not Available |
| `df.index` | — | Not Available |
| `df.T` (transpose) | — | Not Available |

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
Optional<Person> first = dataset.first();
Person last = dataset.last().orElseThrow();
dataset.forEachIndexed((i, row) -> System.out.println(i + ": " + row));
```

---

## Filtering and Selection

| Pandas | Dataset4J | Status |
|--------|-----------|--------|
| `df[condition]` / `df.query()` | `ds.filter(predicate)` | Available |
| `df[df["col"].isin(values)]` | `ds.whereIn(field, values)` | Available |
| `df[~condition]` | `ds.reject(predicate)` | Available |
| `df.dropna(subset=[col])` | `ds.dropNull(field)` | Available |
| `df.dropna()` | — (use multiple `dropNull` calls) | Partial |
| `df.where()` | — (use `filter`) | Partial |
| `df.at[]` / `df.iat[]` | `ds.get(i)` | Partial |
| `df.fillna(value)` | — | Not Available |
| `df.mask()` | — | Not Available |

### Example: Filtering

```python
# Pandas - String-based column access
adults = df[df['age'] > 25]
nyc_or_la = df[df['city'].isin(['NYC', 'LA'])]
non_adults = df[~(df['age'] > 25)]
```

```java
// Dataset4J - Type-safe field access
Dataset<Person> adults = dataset.filter(p -> p.age() > 25);
Dataset<Person> nycOrLa = dataset.whereIn(Person::city, Set.of("NYC", "LA"));
Dataset<Person> nonAdults = dataset.reject(p -> p.age() > 25);
Dataset<Person> noNulls = dataset.dropNull(Person::city);
```

---

## Slicing

| Pandas | Dataset4J | Status |
|--------|-----------|--------|
| `df.head(n)` | `ds.head(n)` | Available |
| `df.tail(n)` | `ds.tail(n)` | Available |
| `df.iloc[a:b]` | `ds.slice(a, b)` | Available |
| `df.sample(n)` | `ds.sample(n)` / `ds.sample(n, seed)` | Available |

---

## Column Operations

| Pandas | Dataset4J | Status |
|--------|-----------|--------|
| `df["col"].tolist()` | `ds.column(field)` | Available |
| `df["col"].tolist()` (int) | `ds.columnInt(field)` | Available |
| `df["col"].tolist()` (double) | `ds.columnDouble(field)` | Available |
| `df.set_index("col")` | `ds.indexBy(field)` | Available |
| `df[["col1", "col2"]]` | — (use `map` to project fields) | Partial |
| `df.drop(columns=[...])` | — (use `map` to exclude fields) | Partial |
| `df['new_col'] = expr` | Map to new record type with additional field | Partial |
| `df.rename(columns={})` | Create new record type with renamed field | Partial |
| `df.reset_index()` | — | Not Available |

### Example: Column Extraction

```python
# Pandas
names = df['name'].tolist()
df.set_index('name', inplace=True)
```

```java
// Dataset4J
List<String> names = dataset.column(Person::name);
int[] ages = dataset.columnInt(Person::age);
Map<String, Person> indexed = dataset.indexBy(Person::name);
```

---

## Sorting Operations

| Pandas | Dataset4J | Status |
|--------|-----------|--------|
| `df.sort_values("col")` | `ds.sortBy(field)` / `ds.sortByInt(field)` / `ds.sortByDouble(field)` | Available |
| `df.sort_values("col", ascending=False)` | `ds.sortByDesc(field)` / `ds.sortByIntDesc(field)` | Available |
| `df.sort_values(by=[...], key=...)` | `ds.sortBy(comparator)` | Available |
| `df.sort_index()` | — | Not Available |
| `df.rank()` | — | Not Available |

### Example: Sorting

```python
# Pandas
sorted_by_age = df.sort_values('age')
sorted_desc = df.sort_values('age', ascending=False)
sorted_multi = df.sort_values(['city', 'age'])
```

```java
// Dataset4J - Type-safe method references
Dataset<Person> sortedByAge = dataset.sortBy(Person::age);
Dataset<Person> sortedDesc = dataset.sortByDesc(Person::age);
Dataset<Person> sortedMulti = dataset
    .sortBy(Comparator.comparing(Person::city).thenComparing(Person::age));
```

---

## Transformation

| Pandas | Dataset4J | Status |
|--------|-----------|--------|
| `df.apply(fn, axis=1)` / `df.assign()` | `ds.map(fn)` | Available |
| `df.explode()` / `df.melt()` | `ds.flatMap(fn)` | Available |
| `df.pipe(fn)` | `ds.pipe(fn)` | Available |
| `df.rename()` | — (use `map` to create new record) | Partial |
| `df.astype()` | — (use `map` to convert types) | Partial |
| `df.replace()` | — (use `map`) | Partial |
| `df.applymap()` | — | Not Available |
| `df.transform()` | — | Not Available |
| `df.eval()` | — | Not Available |
| `df.stack()` / `df.unstack()` | — | Not Available |

### Example: Transformation

```python
# Pandas
df['age_doubled'] = df['age'] * 2
```

```java
// Dataset4J - Requires new record types for schema changes
record PersonWithDouble(String name, int age, String city, int ageDoubled) {}
Dataset<PersonWithDouble> withDoubled = dataset
    .map(p -> new PersonWithDouble(p.name(), p.age(), p.city(), p.age() * 2));
```

---

## Deduplication

| Pandas | Dataset4J | Status |
|--------|-----------|--------|
| `df.drop_duplicates(subset=[col])` | `ds.distinctBy(field)` | Available |
| `df.drop_duplicates(subset=[c1, c2])` | `ds.distinctBy(field1, field2)` | Available |
| `df.drop_duplicates(subset=[c1, c2, c3])` | `ds.distinctBy(field1, field2, field3)` | Available |
| `df.drop_duplicates()` | `ds.distinct()` | Available |
| `df.duplicated()` | — | Not Available |

---

## Grouping and Aggregation

| Pandas | Dataset4J | Status |
|--------|-----------|--------|
| `df.groupby("col")` | `ds.groupBy(field)` | Available |
| `df.groupby(["c1", "c2"])` | `ds.groupBy(f1, f2)` / `ds.groupBy(f1, f2, f3)` | Available |
| `grouped.size()` | `grouped.counts()` | Available |
| `grouped.ngroups` | `grouped.size()` | Available |
| `grouped.groups.keys()` | `grouped.keys()` | Available |
| `grouped.get_group(key)` | `grouped.get(key)` | Available |
| `grouped["col"].mean()` | `grouped.meanInt(field)` | Available |
| `grouped["col"].sum()` | `grouped.sumInt(field)` | Available |
| `grouped["col"].max()` | `grouped.maxInt(field)` | Available |
| `grouped["col"].min()` | `grouped.minInt(field)` | Available |
| `grouped["col"].nunique()` | `grouped.countDistinct(field)` | Available |
| `grouped.agg(fn)` | `grouped.aggregate(fn)` | Available |
| `grouped.apply(fn)` | `grouped.apply(fn)` | Available |
| `grouped.transform(fn)` | `grouped.computePerGroup(fn)` | Available |
| `grouped.first()` / `grouped.last()` | — (use `aggregate`) | Partial |
| `grouped.filter(fn)` | — | Not Available |
| `grouped.cumsum()` | — | Not Available |
| `grouped.rank()` | — | Not Available |

### Aggregation (non-grouped)

| Pandas | Dataset4J | Status |
|--------|-----------|--------|
| `df["col"].sum()` | `ds.sumInt(field)` / `ds.sumLong(field)` / `ds.sumDouble(field)` | Available |
| `df["col"].mean()` | `ds.meanInt(field)` / `ds.meanDouble(field)` | Available |
| `df["col"].max()` | `ds.maxInt(field)` / `ds.maxDouble(field)` / `ds.maxBy(field)` | Available |
| `df["col"].min()` | `ds.minInt(field)` / `ds.minBy(field)` | Available |
| `df["col"].describe()` | `ds.statsInt(field)` / `ds.statsDouble(field)` | Available |
| `df["col"].nunique()` | `ds.countDistinct(field)` / `ds.countDistinct(f1, f2)` / `ds.countDistinct(f1, f2, f3)` | Available |
| `df["col"].value_counts()` | `ds.valueCounts(field)` | Available |
| `df.nlargest(n, "col")` | `ds.nLargest(n, comparator)` | Available |
| `df.nsmallest(n, "col")` | `ds.nSmallest(n, comparator)` | Available |
| `(df["col"] > 0).any()` | `ds.any(predicate)` | Available |
| `(df["col"] > 0).all()` | `ds.all(predicate)` | Available |
| `df[condition].count()` | `ds.count(predicate)` | Available |
| `df.agg(["sum", "mean"])` | — (use individual methods) | Partial |
| `df["col"].median()` | — | Not Available |
| `df["col"].std()` / `.var()` | — | Not Available |
| `df["col"].quantile()` | — | Not Available |
| `df["col"].mode()` | — | Not Available |
| `df.corr()` / `df.cov()` | — | Not Available |

### Example: Grouping

```python
# Pandas
city_counts = df.groupby('city').size()
avg_age_by_city = df.groupby('city')['age'].mean()
```

```java
// Dataset4J
Map<String, Integer> cityCounts = dataset.groupBy(Person::city).counts();
Map<String, Double> avgAge = dataset.groupBy(Person::city).meanInt(Person::age);

// Custom aggregation
record CitySummary(String city, int count, double avgAge) {}
Dataset<CitySummary> summary = dataset.groupBy(Person::city)
    .aggregate((city, group) -> new CitySummary(
        city, group.size(), group.meanInt(Person::age).orElse(0)));
```

---

## Joining Operations

| Pandas | Dataset4J | Status |
|--------|-----------|--------|
| `pd.merge(how="inner")` | `ds.innerJoin(right, leftKey, rightKey)` | Available |
| `pd.merge(how="left")` | `ds.leftJoin(right, leftKey, rightKey)` | Available |
| `pd.merge(how="right")` | `ds.rightJoin(right, leftKey, rightKey)` | Available |
| `pd.merge(how="cross")` | `ds.crossJoin(right)` | Available |
| Inner join with mapper | `ds.innerJoin(right, lk, rk, joiner)` | Available |
| Left join with mapper | `ds.leftJoin(right, lk, rk, joiner)` | Available |
| Multi-key inner join | `ds.innerJoin2(...)` / `ds.innerJoin3Keys(...)` / `ds.innerJoinMulti(...)` / `ds.innerJoinOn(...)` | Available |
| Multi-key left join | `ds.leftJoin2(...)` / `ds.leftJoinMulti(...)` / `ds.leftJoinOn(...)` | Available |
| Multi-key right join | `ds.rightJoinMulti(...)` / `ds.rightJoinOn(...)` | Available |
| `pd.merge(on=...)` (same col) | — (specify both key extractors) | Partial |
| `pd.merge(how="outer")` | — | Not Available |
| `pd.merge_asof()` | — | Not Available |

### Example: Joining

```python
# Pandas
joined = df_employees.merge(df_salaries, on='emp_id')
```

```java
// Dataset4J - Type-safe join with function references
record Employee(int empId, String name) {}
record Salary(int empId, double salary) {}

// Pair-based (no custom record needed)
Dataset<Pair<Employee, Salary>> joined = employees
    .innerJoin(salaries, Employee::empId, Salary::empId);

// With mapper
record EmployeeSalary(int empId, String name, double salary) {}
Dataset<EmployeeSalary> mapped = employees
    .innerJoin(salaries, Employee::empId, Salary::empId,
        (e, s) -> new EmployeeSalary(e.empId(), e.name(), s.salary()));

// Multi-key join with fluent API
import static dataset4j.CompositeKey.on;
employees.innerJoinOn(departments,
    on(Employee::dept, Employee::location),
    on(Department::dept, Department::location));
```

---

## Statistical Operations

| Pandas | Dataset4J | Status |
|--------|-----------|--------|
| `df["col"].sum()` | `ds.sumInt(f)` / `ds.sumLong(f)` / `ds.sumDouble(f)` | Available |
| `df["col"].mean()` | `ds.meanInt(f)` / `ds.meanDouble(f)` | Available |
| `df["col"].max()` | `ds.maxInt(f)` / `ds.maxDouble(f)` / `ds.maxBy(f)` | Available |
| `df["col"].min()` | `ds.minInt(f)` / `ds.minBy(f)` | Available |
| `df["col"].describe()` | `ds.statsInt(f)` / `ds.statsDouble(f)` | Available |
| `df["col"].count()` | `ds.size()` / `ds.count(predicate)` | Available |
| `df["col"].median()` | — | Not Available |
| `df["col"].std()` / `.var()` | — | Not Available |
| `df["col"].quantile()` | — | Not Available |
| `df.corr()` / `df.cov()` | — | Not Available |

### Example: Statistics

```python
# Pandas
age_stats = df['age'].describe()
```

```java
// Dataset4J
IntSummaryStatistics stats = dataset.statsInt(Person::age);
// stats.getSum(), stats.getAverage(), stats.getMax(), stats.getMin(), stats.getCount()

int total = dataset.sumInt(Person::age);
OptionalDouble avg = dataset.meanInt(Person::age);
Optional<Person> oldest = dataset.maxBy(Person::age);
```

---

## Window and Stateful Operations

| Pandas | Dataset4J | Status |
|--------|-----------|--------|
| `df["col"].cumsum()` | `ds.cumSumInt(field)` | Available |
| `df["col"].rolling(w).mean()` | `ds.rollingMeanInt(field, window)` | Available |
| `df["col"].shift(n)` | `ds.shift(field, n)` | Available |
| `df["col"].fillna(method="ffill")` | `ds.forwardFill(field, withValue)` | Available |
| `df["col"].rolling(w).sum()` | — | Not Available |
| `df["col"].rolling(w).std()` | — | Not Available |
| `df["col"].ewm()` | — | Not Available |
| `df["col"].diff()` | — | Not Available |
| `df["col"].pct_change()` | — | Not Available |

---

## String Operations

| Pandas | Dataset4J | Status |
|--------|-----------|--------|
| `df["col"].str.contains()` | — (use `filter` with lambda) | Partial |
| `df["col"].str.startswith()` | — (use `filter` with lambda) | Partial |
| `df["col"].str.lower()` / `.upper()` | — (use `map` with lambda) | Partial |
| `df["col"].str.strip()` | — (use `map` with lambda) | Partial |
| `df["col"].str.replace()` | — (use `map` with lambda) | Partial |
| `df["col"].str.split()` | — | Not Available |

String operations in Dataset4j are handled through Java's native String methods within lambdas:

```java
// Filter by string content
dataset.filter(p -> p.name().contains("John"));
dataset.filter(p -> p.city().startsWith("N"));

// Transform strings
dataset.map(p -> new Person(p.name().toLowerCase(), p.age(), p.city().strip()));
```

---

## Reshaping

| Pandas | Dataset4J | Status |
|--------|-----------|--------|
| `df.melt()` | `ds.flatMap(fn)` | Partial |
| `df.pivot()` | — | Not Available |
| `df.pivot_table()` | — | Not Available |
| `df.stack()` / `df.unstack()` | — | Not Available |
| `pd.get_dummies()` | — | Not Available |

---

## Data Export and Display

| Pandas | Dataset4J | Status |
|--------|-----------|--------|
| `print(df)` / `df.head()` | `ds.toString()` (shows first 10 rows) | Available |
| `df.to_string()` | `ds.toString()` | Available |
| `df.to_csv()` | `CsvDatasetWriter.toFile("f.csv").write(ds)` | Available |
| `df.to_excel()` | `ExcelDatasetWriter.toFile("f.xlsx").write(ds)` | Available |
| `df.to_parquet()` | `ParquetDatasetWriter.toFile("f.parquet").write(ds)` | Available |
| `df.describe()` | `ds.statsInt(field)` / `ds.statsDouble(field)` | Partial |
| `df.to_dict()` | — | Not Available |
| `df.to_numpy()` | — | Not Available |
| `df.to_markdown()` | — | Not Available |

### Example: Export with Field Selection

```python
# Pandas
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
```

---

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

---

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
df.dropna(inplace=True)
df.sort_values('age', inplace=True)
```

```java
// Dataset4J - Immutable chain
Dataset<Person> cleaned = dataset
    .dropNull(Person::city)
    .sortBy(Person::age);
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

---

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
    .sortByDesc(CustomerTotal::total);

ExcelDatasetWriter
    .toFile("high_value_customers.xlsx")
    .fields(CustomerTotal.Fields.CUSTOMER, CustomerTotal.Fields.TOTAL)
    .write(result);
```

---

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

---

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
