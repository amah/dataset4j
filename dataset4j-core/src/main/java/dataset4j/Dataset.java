package dataset4j;

import java.lang.reflect.RecordComponent;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

/**
 * A lightweight DataFrame-like wrapper around List&lt;T&gt; for porting
 * Pandas data processing pipelines to Java.
 *
 * T is typically a Java record representing a row.
 *
 * <pre>
 * record Employee(String name, int age, String dept) {}
 *
 * var ds = Dataset.of(
 *     new Employee("Alice", 30, "Eng"),
 *     new Employee("Bob",   25, "Sales")
 * );
 *
 * ds.filter(e -> e.age() > 25)
 *   .sortBy(Employee::age)
 *   .map(e -> new EmpSalary(e.name(), e.age() * 1000))
 *   .groupBy(EmpSalary::name)
 *   .forEach((k, v) -> System.out.println(k + " -> " + v));
 * </pre>
 */
public class Dataset<T> implements Iterable<T> {

    private final List<T> rows;

    // ---------------------------------------------------------------
    // Construction
    // ---------------------------------------------------------------

    private Dataset(List<T> rows) {
        this.rows = List.copyOf(rows); // immutable snapshot
    }

    public static <T> Dataset<T> of(List<T> rows) {
        return new Dataset<>(rows);
    }

    @SafeVarargs
    public static <T> Dataset<T> of(T... rows) {
        return new Dataset<>(List.of(rows));
    }

    public static <T> Dataset<T> empty() {
        return new Dataset<>(List.of());
    }

    /** Concatenate multiple datasets (pd.concat) */
    @SafeVarargs
    public static <T> Dataset<T> concat(Dataset<T>... datasets) {
        List<T> all = new ArrayList<>();
        for (var ds : datasets) all.addAll(ds.rows);
        return new Dataset<>(all);
    }

    /** Instance concat: this.concat(other) — appends another dataset's rows to this one */
    public Dataset<T> concat(Dataset<T> other) {
        List<T> all = new ArrayList<>(rows.size() + other.rows.size());
        all.addAll(rows);
        all.addAll(other.rows);
        return new Dataset<>(all);
    }

    /**
     * Returns a copy of this Dataset.
     * Since Dataset is immutable, this simply returns the same instance.
     * 
     * This method is provided for API consistency and to make the immutable
     * nature of Dataset explicit.
     * 
     * @return this Dataset instance (no actual copy is needed)
     */
    public Dataset<T> copy() {
        return this;
    }

    /**
     * Convert this typed Dataset to an untyped {@link Table}.
     *
     * <p>Record components become columns; values are auto-wrapped into
     * {@link CellValue} instances preserving their Java type as the
     * corresponding {@link ValueType}.
     *
     * @return a Table representation of this dataset
     */
    public Table toTable() {
        if (rows.isEmpty()) return Table.empty();

        T sample = rows.get(0);
        if (!sample.getClass().isRecord()) {
            throw new IllegalStateException("toTable() requires a record-based Dataset");
        }

        RecordComponent[] components = sample.getClass().getRecordComponents();
        java.util.List<String> columnNames = new java.util.ArrayList<>(components.length);
        for (RecordComponent c : components) {
            columnNames.add(c.getName());
        }

        java.util.List<java.util.Map<String, CellValue>> tableRows = new java.util.ArrayList<>(rows.size());
        for (T record : rows) {
            java.util.Map<String, CellValue> row = new java.util.LinkedHashMap<>();
            for (RecordComponent c : components) {
                try {
                    Object value = c.getAccessor().invoke(record);
                    row.put(c.getName(), Table.wrapValue(value));
                } catch (ReflectiveOperationException e) {
                    row.put(c.getName(), CellValue.blank());
                }
            }
            tableRows.add(row);
        }

        return Table.of(columnNames, tableRows);
    }

    // ---------------------------------------------------------------
    // Basic properties
    // ---------------------------------------------------------------

    /** Number of rows (len(df)) */
    public int size() {
        return rows.size();
    }

    /** df.empty */
    public boolean isEmpty() {
        return rows.isEmpty();
    }

    /** Access underlying immutable list */
    public List<T> toList() {
        return rows;
    }

    /** df.iloc[i] */
    public T get(int index) {
        return rows.get(index);
    }

    /** df.iloc[0] */
    public Optional<T> first() {
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    /** df.iloc[-1] */
    public Optional<T> last() {
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(rows.size() - 1));
    }

    @Override
    public Iterator<T> iterator() {
        return rows.iterator();
    }

    public Stream<T> stream() {
        return rows.stream();
    }

    // ---------------------------------------------------------------
    // Filtering  (df[df["age"] > 25], df.query(...))
    // ---------------------------------------------------------------

    /** df[condition] / df.query(...) */
    public Dataset<T> filter(Predicate<T> predicate) {
        return new Dataset<>(rows.stream().filter(predicate).toList());
    }

    /** df[df["col"].isin(values)] */
    public <V> Dataset<T> whereIn(Function<T, V> field, Collection<V> values) {
        Set<V> set = values instanceof Set ? (Set<V>) values : new HashSet<>(values);
        return filter(row -> set.contains(field.apply(row)));
    }

    /** df[~condition] */
    public Dataset<T> reject(Predicate<T> predicate) {
        return filter(predicate.negate());
    }

    /** df.dropna(subset=[col]) — drop rows where field is null */
    public Dataset<T> dropNull(Function<T, ?> field) {
        return filter(row -> field.apply(row) != null);
    }

    // ---------------------------------------------------------------
    // Slicing  (df.head(), df.tail(), df.iloc[a:b])
    // ---------------------------------------------------------------

    /** df.head(n) */
    public Dataset<T> head(int n) {
        return new Dataset<>(rows.subList(0, Math.min(n, rows.size())));
    }

    /** df.tail(n) */
    public Dataset<T> tail(int n) {
        int from = Math.max(0, rows.size() - n);
        return new Dataset<>(rows.subList(from, rows.size()));
    }

    /** df.iloc[from:to] */
    public Dataset<T> slice(int from, int to) {
        return new Dataset<>(rows.subList(
            Math.max(0, from),
            Math.min(to, rows.size())
        ));
    }

    /** df.sample(n) */
    public Dataset<T> sample(int n) {
        var shuffled = new ArrayList<>(rows);
        Collections.shuffle(shuffled);
        return Dataset.of(shuffled).head(n);
    }

    /** df.sample(n, random_state=seed) */
    public Dataset<T> sample(int n, long seed) {
        var shuffled = new ArrayList<>(rows);
        Collections.shuffle(shuffled, new Random(seed));
        return Dataset.of(shuffled).head(n);
    }

    // ---------------------------------------------------------------
    // Sorting  (df.sort_values(...))
    // ---------------------------------------------------------------

    /** df.sort_values("col") */
    public Dataset<T> sortBy(Comparator<T> comparator) {
        return new Dataset<>(rows.stream().sorted(comparator).toList());
    }

    /** df.sort_values("col") for Comparable fields */
    public <U extends Comparable<U>> Dataset<T> sortBy(Function<T, U> key) {
        return sortBy(Comparator.comparing(key));
    }

    /** df.sort_values("col", ascending=False) */
    public <U extends Comparable<U>> Dataset<T> sortByDesc(Function<T, U> key) {
        return sortBy(Comparator.comparing(key).reversed());
    }

    /** df.sort_values("col") for int fields */
    public Dataset<T> sortByInt(ToIntFunction<T> key) {
        return sortBy(Comparator.comparingInt(key));
    }

    /** df.sort_values("col", ascending=False) for int fields */
    public Dataset<T> sortByIntDesc(ToIntFunction<T> key) {
        return sortBy(Comparator.comparingInt(key).reversed());
    }

    /** df.sort_values("col") for double fields */
    public Dataset<T> sortByDouble(ToDoubleFunction<T> key) {
        return sortBy(Comparator.comparingDouble(key));
    }

    // ---------------------------------------------------------------
    // Transformation  (df.assign(...), df.apply(...))
    // ---------------------------------------------------------------

    /** df.assign(...) / df.apply(fn, axis=1) — transform each row */
    public <R> Dataset<R> map(Function<T, R> mapper) {
        return new Dataset<>(rows.stream().map(mapper).toList());
    }

    /** df.reset_index() / df.assign(row_num=...) — map with row index (0-based) */
    public <R> Dataset<R> mapIndexed(BiFunction<Integer, T, R> mapper) {
        List<R> result = new ArrayList<>(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            result.add(mapper.apply(i, rows.get(i)));
        }
        return new Dataset<>(result);
    }

    /** Explode / melt — one row becomes many */
    public <R> Dataset<R> flatMap(Function<T, Collection<R>> mapper) {
        return new Dataset<>(rows.stream()
            .flatMap(row -> mapper.apply(row).stream())
            .toList());
    }

    // ---------------------------------------------------------------
    // Deduplication  (df.drop_duplicates(...))
    // ---------------------------------------------------------------

    /** df.drop_duplicates(subset=[col]) — keeps first occurrence */
    public <K> Dataset<T> distinctBy(Function<T, K> key) {
        Set<K> seen = new LinkedHashSet<>();
        return new Dataset<>(rows.stream()
            .filter(row -> seen.add(key.apply(row)))
            .toList());
    }

    /** df.drop_duplicates(subset=[col1, col2]) — deduplicate on 2 keys */
    public <K1, K2> Dataset<T> distinctBy(Function<T, K1> key1, Function<T, K2> key2) {
        Set<CompositeKey> seen = new LinkedHashSet<>();
        return new Dataset<>(rows.stream()
            .filter(row -> seen.add(CompositeKey.of(key1.apply(row), key2.apply(row))))
            .toList());
    }

    /** df.drop_duplicates(subset=[col1, col2, col3]) — deduplicate on 3 keys */
    public <K1, K2, K3> Dataset<T> distinctBy(Function<T, K1> key1, Function<T, K2> key2, Function<T, K3> key3) {
        Set<CompositeKey> seen = new LinkedHashSet<>();
        return new Dataset<>(rows.stream()
            .filter(row -> seen.add(CompositeKey.of(key1.apply(row), key2.apply(row), key3.apply(row))))
            .toList());
    }

    /** df.drop_duplicates(subset=[col1, ..., colN]) — deduplicate on N keys */
    @SafeVarargs
    public final Dataset<T> distinctBy(Function<T, ?>... keys) {
        if (keys.length == 0) throw new IllegalArgumentException("At least one key is required");
        Set<CompositeKey> seen = new LinkedHashSet<>();
        return new Dataset<>(rows.stream()
            .filter(row -> {
                Object[] values = new Object[keys.length];
                for (int i = 0; i < keys.length; i++) values[i] = keys[i].apply(row);
                return seen.add(CompositeKey.of(values));
            })
            .toList());
    }

    /** df.drop_duplicates() — uses equals() */
    public Dataset<T> distinct() {
        return new Dataset<>(rows.stream().distinct().toList());
    }

    // ---------------------------------------------------------------
    // Aggregation  (df["col"].sum(), .mean(), etc.)
    // ---------------------------------------------------------------

    /** df["col"].sum() */
    public int sumInt(ToIntFunction<T> field) {
        return rows.stream().mapToInt(field).sum();
    }

    public long sumLong(ToLongFunction<T> field) {
        return rows.stream().mapToLong(field).sum();
    }

    public double sumDouble(ToDoubleFunction<T> field) {
        return rows.stream().mapToDouble(field).sum();
    }

    /** df["col"].mean() */
    public OptionalDouble meanInt(ToIntFunction<T> field) {
        return rows.stream().mapToInt(field).average();
    }

    public OptionalDouble meanDouble(ToDoubleFunction<T> field) {
        return rows.stream().mapToDouble(field).average();
    }

    /** df["col"].max() */
    public OptionalInt maxInt(ToIntFunction<T> field) {
        return rows.stream().mapToInt(field).max();
    }

    public OptionalDouble maxDouble(ToDoubleFunction<T> field) {
        return rows.stream().mapToDouble(field).max();
    }

    public <U extends Comparable<U>> Optional<T> maxBy(Function<T, U> key) {
        return rows.stream().max(Comparator.comparing(key));
    }

    /** df["col"].min() */
    public OptionalInt minInt(ToIntFunction<T> field) {
        return rows.stream().mapToInt(field).min();
    }

    public <U extends Comparable<U>> Optional<T> minBy(Function<T, U> key) {
        return rows.stream().min(Comparator.comparing(key));
    }

    /** df["col"].describe() */
    public IntSummaryStatistics statsInt(ToIntFunction<T> field) {
        return rows.stream().mapToInt(field).summaryStatistics();
    }

    public DoubleSummaryStatistics statsDouble(ToDoubleFunction<T> field) {
        return rows.stream().mapToDouble(field).summaryStatistics();
    }

    /** df["col"].nunique() */
    public <V> long countDistinct(Function<T, V> field) {
        return rows.stream().map(field).distinct().count();
    }

    /** df[["col1", "col2"]].nunique() — count distinct combinations of 2 fields */
    public <V1, V2> long countDistinct(Function<T, V1> field1, Function<T, V2> field2) {
        return rows.stream()
            .map(row -> CompositeKey.of(field1.apply(row), field2.apply(row)))
            .distinct().count();
    }

    /** df[["col1", "col2", "col3"]].nunique() — count distinct combinations of 3 fields */
    public <V1, V2, V3> long countDistinct(Function<T, V1> field1, Function<T, V2> field2, Function<T, V3> field3) {
        return rows.stream()
            .map(row -> CompositeKey.of(field1.apply(row), field2.apply(row), field3.apply(row)))
            .distinct().count();
    }

    /** df["col"].value_counts() */
    public <V> Map<V, Long> valueCounts(Function<T, V> field) {
        return rows.stream()
            .collect(Collectors.groupingBy(field, Collectors.counting()));
    }

    /** df.nlargest(n, "col") */
    public Dataset<T> nLargest(int n, Comparator<T> comparator) {
        return sortBy(comparator.reversed()).head(n);
    }

    /** df.nsmallest(n, "col") */
    public Dataset<T> nSmallest(int n, Comparator<T> comparator) {
        return sortBy(comparator).head(n);
    }

    // ---------------------------------------------------------------
    // Column extraction  (df["col"])
    // ---------------------------------------------------------------

    /** df["col"].tolist() */
    public <V> List<V> column(Function<T, V> field) {
        return rows.stream().map(field).toList();
    }

    /** df["col"].tolist() for int fields (unboxed) */
    public int[] columnInt(ToIntFunction<T> field) {
        return rows.stream().mapToInt(field).toArray();
    }

    /** df["col"].tolist() for double fields (unboxed) */
    public double[] columnDouble(ToDoubleFunction<T> field) {
        return rows.stream().mapToDouble(field).toArray();
    }

    /** df.set_index("col") — build a lookup map */
    public <K> Map<K, T> indexBy(Function<T, K> key) {
        return rows.stream().collect(Collectors.toMap(
            key, row -> row, (a, b) -> b, LinkedHashMap::new
        ));
    }

    // ---------------------------------------------------------------
    // GroupBy  (df.groupby("col"))
    // ---------------------------------------------------------------

    /** df.groupby("col") — returns grouped map of Datasets */
    public <K> GroupedDataset<K, T> groupBy(Function<T, K> key) {
        Map<K, List<T>> groups = rows.stream()
            .collect(Collectors.groupingBy(key, LinkedHashMap::new, Collectors.toList()));
        return new GroupedDataset<>(groups);
    }

    /**
     * df.groupby(["col1", "col2"]) — group by 2 keys using CompositeKey.
     *
     * <pre>
     * employees.groupBy(Employee::dept, Employee::location);
     * </pre>
     */
    public <K1, K2> GroupedDataset<CompositeKey, T> groupBy(
            Function<T, K1> key1, Function<T, K2> key2) {
        return groupBy(row -> CompositeKey.of(key1.apply(row), key2.apply(row)));
    }

    /**
     * df.groupby(["col1", "col2", "col3"]) — group by 3 keys using CompositeKey.
     */
    public <K1, K2, K3> GroupedDataset<CompositeKey, T> groupBy(
            Function<T, K1> key1, Function<T, K2> key2, Function<T, K3> key3) {
        return groupBy(row -> CompositeKey.of(key1.apply(row), key2.apply(row), key3.apply(row)));
    }

    /**
     * df.groupby(["col1", "col2"]) — group by CompositeKey factory (fluent API).
     *
     * <pre>
     * import static dataset4j.CompositeKey.on;
     * employees.groupByOn(on(Employee::dept, Employee::location));
     * </pre>
     */
    public GroupedDataset<CompositeKey, T> groupByOn(Function<T, CompositeKey> keyFactory) {
        Map<CompositeKey, List<T>> groups = rows.stream()
            .collect(Collectors.groupingBy(keyFactory, LinkedHashMap::new, Collectors.toList()));
        return new GroupedDataset<>(groups);
    }

    // ---------------------------------------------------------------
    // Joins  (pd.merge)
    // ---------------------------------------------------------------

    /**
     * Inner join: pd.merge(left, right, on=key)
     *
     * @param right    the other dataset
     * @param leftKey  key extractor from left row
     * @param rightKey key extractor from right row
     * @param joiner   combines matching left + right rows
     */
    public <R, K, J> Dataset<J> innerJoin(
            Dataset<R> right,
            Function<T, K> leftKey,
            Function<R, K> rightKey,
            BiFunction<T, R, J> joiner) {
        Map<K, List<R>> rightIndex = right.rows.stream()
            .collect(Collectors.groupingBy(rightKey));
        return new Dataset<>(rows.stream()
            .flatMap(leftRow -> {
                var matches = rightIndex.getOrDefault(leftKey.apply(leftRow), List.of());
                return matches.stream().map(rightRow -> joiner.apply(leftRow, rightRow));
            })
            .toList());
    }

    /**
     * Left join: pd.merge(left, right, on=key, how="left")
     *
     * joiner receives null for the right side when there's no match.
     */
    public <R, K, J> Dataset<J> leftJoin(
            Dataset<R> right,
            Function<T, K> leftKey,
            Function<R, K> rightKey,
            BiFunction<T, R, J> joiner) {
        Map<K, List<R>> rightIndex = right.rows.stream()
            .collect(Collectors.groupingBy(rightKey));
        return new Dataset<>(rows.stream()
            .flatMap(leftRow -> {
                var matches = rightIndex.get(leftKey.apply(leftRow));
                if (matches == null) {
                    return Stream.of(joiner.apply(leftRow, null));
                }
                return matches.stream().map(rightRow -> joiner.apply(leftRow, rightRow));
            })
            .toList());
    }

    // ---------------------------------------------------------------
    // Joins returning Pair  (no custom record needed)
    // ---------------------------------------------------------------

    /**
     * Inner join returning Pair: pd.merge(left, right, on=key)
     *
     * <pre>
     * Dataset&lt;Pair&lt;Employee, Department&gt;&gt; joined =
     *     employees.innerJoin(departments, Employee::dept, Department::dept);
     *
     * joined.filter(p -> p.left().age() > 25)
     *       .map(p -> p.left().name() + " in " + p.right().location());
     * </pre>
     */
    public <R, K> Dataset<Pair<T, R>> innerJoin(
            Dataset<R> right,
            Function<T, K> leftKey,
            Function<R, K> rightKey) {
        return innerJoin(right, leftKey, rightKey, Pair::new);
    }

    /**
     * Left join returning Pair: pd.merge(left, right, on=key, how="left")
     *
     * {@code pair.right()} is null when there is no match on the right side.
     */
    public <R, K> Dataset<Pair<T, R>> leftJoin(
            Dataset<R> right,
            Function<T, K> leftKey,
            Function<R, K> rightKey) {
        return leftJoin(right, leftKey, rightKey, Pair::new);
    }

    /**
     * Right join returning Pair: pd.merge(left, right, on=key, how="right")
     *
     * {@code pair.left()} is null when there is no match on the left side.
     */
    public <R, K> Dataset<Pair<T, R>> rightJoin(
            Dataset<R> right,
            Function<T, K> leftKey,
            Function<R, K> rightKey) {
        // Flip: right.leftJoin(this), then swap the Pair sides
        Map<K, List<T>> leftIndex = rows.stream()
            .collect(Collectors.groupingBy(leftKey));
        return new Dataset<>(right.rows.stream()
            .flatMap(rightRow -> {
                var matches = leftIndex.get(rightKey.apply(rightRow));
                if (matches == null) {
                    return Stream.of(new Pair<T, R>(null, rightRow));
                }
                return matches.stream().map(leftRow -> new Pair<>(leftRow, rightRow));
            })
            .toList());
    }

    /**
     * Cross join (cartesian product): every left row paired with every right row.
     *
     * <pre>
     * Dataset&lt;Pair&lt;Color, Size&gt;&gt; combos = colors.crossJoin(sizes);
     * </pre>
     */
    public <R> Dataset<Pair<T, R>> crossJoin(Dataset<R> right) {
        return new Dataset<>(rows.stream()
            .flatMap(leftRow -> right.rows.stream()
                .map(rightRow -> new Pair<>(leftRow, rightRow)))
            .toList());
    }


    // ---------------------------------------------------------------
    // Multi-key joins using CompositeKey
    // ---------------------------------------------------------------

    /**
     * Inner join on multiple keys using CompositeKey.
     *
     * <pre>
     * import static dataset4j.CompositeKey.key;
     * 
     * // Join on both dept AND location
     * employees.innerJoinMulti(departments,
     *     e -> key(e.dept(), e.location()),
     *     d -> key(d.dept(), d.location()));
     * </pre>
     */
    public <R> Dataset<Pair<T, R>> innerJoinMulti(
            Dataset<R> right,
            Function<T, CompositeKey> leftKey,
            Function<R, CompositeKey> rightKey) {
        return innerJoin(right, leftKey, rightKey);
    }

    /**
     * Left join on multiple keys using CompositeKey.
     */
    public <R> Dataset<Pair<T, R>> leftJoinMulti(
            Dataset<R> right,
            Function<T, CompositeKey> leftKey,
            Function<R, CompositeKey> rightKey) {
        return leftJoin(right, leftKey, rightKey);
    }

    /**
     * Right join on multiple keys using CompositeKey.
     */
    public <R> Dataset<Pair<T, R>> rightJoinMulti(
            Dataset<R> right,
            Function<T, CompositeKey> leftKey,
            Function<R, CompositeKey> rightKey) {
        return rightJoin(right, leftKey, rightKey);
    }

    /**
     * Inner join on exactly 2 keys (convenience method).
     *
     * <pre>
     * employees.innerJoin2(departments,
     *     Employee::dept, Employee::location,
     *     Department::dept, Department::location);
     * </pre>
     */
    public <R, K1, K2> Dataset<Pair<T, R>> innerJoin2(
            Dataset<R> right,
            Function<T, K1> leftKey1, Function<T, K2> leftKey2,
            Function<R, K1> rightKey1, Function<R, K2> rightKey2) {
        return innerJoin(right,
            row -> CompositeKey.of(leftKey1.apply(row), leftKey2.apply(row)),
            row -> CompositeKey.of(rightKey1.apply(row), rightKey2.apply(row)));
    }

    /**
     * Left join on exactly 2 keys (convenience method).
     */
    public <R, K1, K2> Dataset<Pair<T, R>> leftJoin2(
            Dataset<R> right,
            Function<T, K1> leftKey1, Function<T, K2> leftKey2,
            Function<R, K1> rightKey1, Function<R, K2> rightKey2) {
        return leftJoin(right,
            row -> CompositeKey.of(leftKey1.apply(row), leftKey2.apply(row)),
            row -> CompositeKey.of(rightKey1.apply(row), rightKey2.apply(row)));
    }

    /**
     * Inner join on exactly 3 keys (convenience method).
     */
    public <R, K1, K2, K3> Dataset<Pair<T, R>> innerJoin3Keys(
            Dataset<R> right,
            Function<T, K1> leftKey1, Function<T, K2> leftKey2, Function<T, K3> leftKey3,
            Function<R, K1> rightKey1, Function<R, K2> rightKey2, Function<R, K3> rightKey3) {
        return innerJoin(right,
            row -> CompositeKey.of(leftKey1.apply(row), leftKey2.apply(row), leftKey3.apply(row)),
            row -> CompositeKey.of(rightKey1.apply(row), rightKey2.apply(row), rightKey3.apply(row)));
    }

    // ---------------------------------------------------------------
    // Fluent join methods using CompositeKey.on()
    // ---------------------------------------------------------------

    /**
     * Inner join with fluent key specification using property accessors.
     *
     * <pre>
     * import static dataset4j.CompositeKey.on;
     * 
     * employees.innerJoinOn(departments,
     *     on(Employee::dept, Employee::location),
     *     on(Department::dept, Department::location));
     * </pre>
     */
    public <R> Dataset<Pair<T, R>> innerJoinOn(
            Dataset<R> right,
            Function<T, CompositeKey> leftKeyFactory,
            Function<R, CompositeKey> rightKeyFactory) {
        return innerJoin(right, leftKeyFactory, rightKeyFactory);
    }

    /**
     * Left join with fluent key specification using property accessors.
     */
    public <R> Dataset<Pair<T, R>> leftJoinOn(
            Dataset<R> right,
            Function<T, CompositeKey> leftKeyFactory,
            Function<R, CompositeKey> rightKeyFactory) {
        return leftJoin(right, leftKeyFactory, rightKeyFactory);
    }

    /**
     * Right join with fluent key specification using property accessors.
     */
    public <R> Dataset<Pair<T, R>> rightJoinOn(
            Dataset<R> right,
            Function<T, CompositeKey> leftKeyFactory,
            Function<R, CompositeKey> rightKeyFactory) {
        return rightJoin(right, leftKeyFactory, rightKeyFactory);
    }

    // ---------------------------------------------------------------
    // Stateful / window operations (cumsum, rolling, shift, ffill)
    // These intentionally use loops, not streams.
    // ---------------------------------------------------------------

    /** df["col"].cumsum() — returns cumulative sums */
    public List<Integer> cumSumInt(ToIntFunction<T> field) {
        List<Integer> result = new ArrayList<>(rows.size());
        int running = 0;
        for (var row : rows) {
            running += field.applyAsInt(row);
            result.add(running);
        }
        return result;
    }

    /** df["col"].rolling(window).mean() */
    public List<OptionalDouble> rollingMeanInt(ToIntFunction<T> field, int window) {
        List<OptionalDouble> result = new ArrayList<>(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            if (i < window - 1) {
                result.add(OptionalDouble.empty());
            } else {
                double avg = rows.subList(i - window + 1, i + 1).stream()
                    .mapToInt(field).average().orElse(Double.NaN);
                result.add(OptionalDouble.of(avg));
            }
        }
        return result;
    }

    /** df["col"].shift(n) — returns values shifted by n positions (null-padded) */
    public <V> List<V> shift(Function<T, V> field, int n) {
        List<V> result = new ArrayList<>(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            int srcIndex = i - n;
            result.add(srcIndex >= 0 && srcIndex < rows.size()
                ? field.apply(rows.get(srcIndex))
                : null);
        }
        return result;
    }

    /**
     * df["col"].fillna(method="ffill") — forward fill nulls.
     * Returns a new Dataset with the field filled.
     */
    public <V> Dataset<T> forwardFill(Function<T, V> field, BiFunction<T, V, T> withValue) {
        List<T> filled = new ArrayList<>(rows.size());
        V lastValue = null;
        for (var row : rows) {
            V val = field.apply(row);
            if (val != null) {
                lastValue = val;
                filled.add(row);
            } else {
                filled.add(withValue.apply(row, lastValue));
            }
        }
        return new Dataset<>(filled);
    }

    // ---------------------------------------------------------------
    // Iteration & side effects
    // ---------------------------------------------------------------

    /** df.iterrows() with index */
    public void forEachIndexed(BiConsumer<Integer, T> action) {
        for (int i = 0; i < rows.size(); i++) {
            action.accept(i, rows.get(i));
        }
    }

    /** df.pipe(fn) */
    public <R> R pipe(Function<Dataset<T>, R> fn) {
        return fn.apply(this);
    }

    // ---------------------------------------------------------------
    // Any / All checks
    // ---------------------------------------------------------------

    /** (df["col"] > 0).any() */
    public boolean any(Predicate<T> predicate) {
        return rows.stream().anyMatch(predicate);
    }

    /** (df["col"] > 0).all() */
    public boolean all(Predicate<T> predicate) {
        return rows.stream().allMatch(predicate);
    }

    /** count rows matching condition */
    public long count(Predicate<T> predicate) {
        return rows.stream().filter(predicate).count();
    }

    // ---------------------------------------------------------------
    // Display / print  (print(df), df.to_string())
    // ---------------------------------------------------------------

    /** Print tabular representation to stdout (like pandas print(df)). */
    public void print() {
        System.out.println(toTabularString());
    }

    /** Print tabular representation to stdout with custom row limit. */
    public void print(int maxRows) {
        System.out.println(toTabularString(maxRows));
    }

    /** Return pandas-style tabular string with default row limit (20). */
    public String toTabularString() {
        return toTabularString(20);
    }

    /** Return pandas-style tabular string with custom row limit. */
    public String toTabularString(int maxRows) {
        if (rows.isEmpty()) return "Empty Dataset";
        return buildTabularString(maxRows);
    }

    @Override
    public String toString() {
        return toTabularString();
    }

    private String buildTabularString(int maxRows) {
        T first = rows.get(0);
        Class<?> clazz = first.getClass();

        // Extract column names and accessors
        String[] headers;
        java.util.function.Function<Object, String>[] accessors;

        if (clazz.isRecord()) {
            RecordComponent[] components = clazz.getRecordComponents();
            headers = new String[components.length];
            accessors = new java.util.function.Function[components.length];
            for (int c = 0; c < components.length; c++) {
                headers[c] = getColumnDisplayName(components[c]);
                final var accessor = components[c].getAccessor();
                accessors[c] = row -> {
                    try { Object v = accessor.invoke(row); return v == null ? "" : v.toString(); }
                    catch (Exception e) { return "?"; }
                };
            }
        } else {
            // Non-record: single column with toString
            headers = new String[]{"value"};
            accessors = new java.util.function.Function[]{Object::toString};
        }

        int numCols = headers.length;
        int totalRows = rows.size();
        boolean truncated = totalRows > maxRows;
        int headCount = truncated ? maxRows / 2 : totalRows;
        int tailCount = truncated ? maxRows - headCount : 0;

        // Collect display rows (head + tail)
        List<int[]> displayIndices = new ArrayList<>(); // original index
        List<String[]> displayValues = new ArrayList<>();
        for (int i = 0; i < headCount; i++) {
            displayIndices.add(new int[]{i});
            displayValues.add(extractRow(rows.get(i), accessors));
        }
        if (truncated) {
            int tailStart = totalRows - tailCount;
            for (int i = tailStart; i < totalRows; i++) {
                displayIndices.add(new int[]{i});
                displayValues.add(extractRow(rows.get(i), accessors));
            }
        }

        // Compute column widths
        int indexWidth = String.valueOf(totalRows - 1).length();
        int[] colWidths = new int[numCols];
        boolean[] numeric = new boolean[numCols];
        for (int c = 0; c < numCols; c++) {
            colWidths[c] = headers[c].length();
            numeric[c] = isNumericColumn(clazz, c);
        }
        for (String[] vals : displayValues) {
            for (int c = 0; c < numCols; c++) {
                colWidths[c] = Math.max(colWidths[c], vals[c].length());
            }
        }
        // Ellipsis row widths
        if (truncated) {
            indexWidth = Math.max(indexWidth, 3); // "..."
            for (int c = 0; c < numCols; c++) {
                colWidths[c] = Math.max(colWidths[c], 3);
            }
        }

        // Build output
        StringBuilder sb = new StringBuilder();

        // Header row
        sb.append(pad("", indexWidth, false));
        for (int c = 0; c < numCols; c++) {
            sb.append("  ").append(pad(headers[c], colWidths[c], numeric[c]));
        }
        sb.append('\n');

        // Data rows (head)
        for (int r = 0; r < headCount; r++) {
            String idx = String.valueOf(displayIndices.get(r)[0]);
            sb.append(pad(idx, indexWidth, true));
            String[] vals = displayValues.get(r);
            for (int c = 0; c < numCols; c++) {
                sb.append("  ").append(pad(vals[c], colWidths[c], numeric[c]));
            }
            sb.append('\n');
        }

        // Ellipsis row
        if (truncated) {
            sb.append(pad("...", indexWidth, false));
            for (int c = 0; c < numCols; c++) {
                sb.append("  ").append(pad("...", colWidths[c], numeric[c]));
            }
            sb.append('\n');

            // Tail rows
            for (int r = headCount; r < displayValues.size(); r++) {
                String idx = String.valueOf(displayIndices.get(r)[0]);
                sb.append(pad(idx, indexWidth, true));
                String[] vals = displayValues.get(r);
                for (int c = 0; c < numCols; c++) {
                    sb.append("  ").append(pad(vals[c], colWidths[c], numeric[c]));
                }
                sb.append('\n');
            }
        }

        // Footer
        sb.append('\n').append('[').append(totalRows).append(" rows x ").append(numCols).append(" columns]");
        return sb.toString();
    }

    private String[] extractRow(T row, java.util.function.Function<Object, String>[] accessors) {
        String[] vals = new String[accessors.length];
        for (int c = 0; c < accessors.length; c++) {
            vals[c] = accessors[c].apply(row);
        }
        return vals;
    }

    private static String getColumnDisplayName(RecordComponent component) {
        try {
            var annotation = component.getAnnotation(
                (Class<? extends java.lang.annotation.Annotation>)
                    Class.forName("dataset4j.annotations.DataColumn"));
            if (annotation != null) {
                String name = (String) annotation.annotationType().getMethod("name").invoke(annotation);
                if (name != null && !name.isEmpty()) return name;
            }
        } catch (Exception ignored) {
            // DataColumn not on classpath or not annotated — use field name
        }
        return component.getName();
    }

    private static boolean isNumericColumn(Class<?> clazz, int colIndex) {
        if (!clazz.isRecord()) return false;
        RecordComponent[] components = clazz.getRecordComponents();
        if (colIndex >= components.length) return false;
        Class<?> type = components[colIndex].getType();
        return type == int.class || type == Integer.class
            || type == long.class || type == Long.class
            || type == double.class || type == Double.class
            || type == float.class || type == Float.class
            || type == java.math.BigDecimal.class;
    }

    private static String pad(String value, int width, boolean rightAlign) {
        if (value.length() >= width) return value;
        int padding = width - value.length();
        if (rightAlign) {
            return " ".repeat(padding) + value;
        } else {
            return value + " ".repeat(padding);
        }
    }

    // ===============================================================
    // GroupedDataset — result of groupBy()
    // ===============================================================

    public static class GroupedDataset<K, T> {
        private final Map<K, List<T>> groups;

        GroupedDataset(Map<K, List<T>> groups) {
            this.groups = groups;
        }

        /** Number of groups */
        public int size() {
            return groups.size();
        }

        /** Get keys */
        public Set<K> keys() {
            return groups.keySet();
        }

        /** Get a specific group as a Dataset */
        public Dataset<T> get(K key) {
            return Dataset.of(groups.getOrDefault(key, List.of()));
        }

        /** df.groupby("col").size() */
        public Map<K, Integer> counts() {
            Map<K, Integer> result = new LinkedHashMap<>();
            groups.forEach((k, v) -> result.put(k, v.size()));
            return result;
        }

        /** df.groupby("col")["field"].mean() */
        public Map<K, Double> meanInt(ToIntFunction<T> field) {
            Map<K, Double> result = new LinkedHashMap<>();
            groups.forEach((k, v) -> result.put(k,
                v.stream().mapToInt(field).average().orElse(Double.NaN)));
            return result;
        }

        /** df.groupby("col")["field"].sum() */
        public Map<K, Integer> sumInt(ToIntFunction<T> field) {
            Map<K, Integer> result = new LinkedHashMap<>();
            groups.forEach((k, v) -> result.put(k,
                v.stream().mapToInt(field).sum()));
            return result;
        }

        /** df.groupby("col")["field"].max() */
        public Map<K, OptionalInt> maxInt(ToIntFunction<T> field) {
            Map<K, OptionalInt> result = new LinkedHashMap<>();
            groups.forEach((k, v) -> result.put(k,
                v.stream().mapToInt(field).max()));
            return result;
        }

        /** df.groupby("col")["field"].min() */
        public Map<K, OptionalInt> minInt(ToIntFunction<T> field) {
            Map<K, OptionalInt> result = new LinkedHashMap<>();
            groups.forEach((k, v) -> result.put(k,
                v.stream().mapToInt(field).min()));
            return result;
        }

        /**
         * df.groupby("col").agg(...) — custom aggregation per group.
         * The aggregator receives each group as a Dataset and returns a result row.
         */
        public <R> Dataset<R> aggregate(BiFunction<K, Dataset<T>, R> aggregator) {
            return Dataset.of(groups.entrySet().stream()
                .map(e -> aggregator.apply(e.getKey(), Dataset.of(e.getValue())))
                .toList());
        }

        /**
         * df.groupby("col").apply(fn) — transform each group.
         */
        public Dataset<T> apply(Function<Dataset<T>, Dataset<T>> transform) {
            List<T> result = new ArrayList<>();
            groups.forEach((k, v) -> result.addAll(transform.apply(Dataset.of(v)).toList()));
            return Dataset.of(result);
        }

        /**
         * df.groupby("col")["field"].transform("mean") —
         * broadcast aggregated value back to each row.
         */
        public <V> Map<K, V> computePerGroup(Function<Dataset<T>, V> computation) {
            Map<K, V> result = new LinkedHashMap<>();
            groups.forEach((k, v) -> result.put(k, computation.apply(Dataset.of(v))));
            return result;
        }

        /** df.groupby("col")["field"].nunique() — count distinct values per group */
        public <V> Map<K, Long> countDistinct(Function<T, V> field) {
            Map<K, Long> result = new LinkedHashMap<>();
            groups.forEach((k, v) -> result.put(k,
                v.stream().map(field).distinct().count()));
            return result;
        }

        /** df.groupby("col")[["f1","f2"]].nunique() — count distinct combinations per group */
        public <V1, V2> Map<K, Long> countDistinct(Function<T, V1> field1, Function<T, V2> field2) {
            Map<K, Long> result = new LinkedHashMap<>();
            groups.forEach((k, v) -> result.put(k,
                v.stream()
                    .map(row -> CompositeKey.of(field1.apply(row), field2.apply(row)))
                    .distinct().count()));
            return result;
        }

        /** df.groupby("col")[["f1","f2","f3"]].nunique() — count distinct combinations per group */
        public <V1, V2, V3> Map<K, Long> countDistinct(Function<T, V1> field1, Function<T, V2> field2, Function<T, V3> field3) {
            Map<K, Long> result = new LinkedHashMap<>();
            groups.forEach((k, v) -> result.put(k,
                v.stream()
                    .map(row -> CompositeKey.of(field1.apply(row), field2.apply(row), field3.apply(row)))
                    .distinct().count()));
            return result;
        }

        /** Iterate over groups */
        public void forEach(BiConsumer<K, Dataset<T>> action) {
            groups.forEach((k, v) -> action.accept(k, Dataset.of(v)));
        }

        /** Get as map of Datasets */
        public Map<K, Dataset<T>> toMap() {
            Map<K, Dataset<T>> result = new LinkedHashMap<>();
            groups.forEach((k, v) -> result.put(k, Dataset.of(v)));
            return result;
        }
    }
}
