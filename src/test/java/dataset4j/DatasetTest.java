package dataset4j;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.*;

import static dataset4j.CompositeKey.key;
import static dataset4j.CompositeKey.on;
import static org.junit.jupiter.api.Assertions.*;

class DatasetTest {

    record Employee(String name, int age, String dept) {}
    record Department(String dept, String location) {}
    record Budget(String dept, int amount) {}

    static final Dataset<Employee> EMPLOYEES = Dataset.of(
        new Employee("Alice",   30, "Eng"),
        new Employee("Bob",     25, "Sales"),
        new Employee("Charlie", 35, "Eng"),
        new Employee("Diana",   28, "Sales"),
        new Employee("Eve",     32, "Eng")
    );

    // -----------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------

    @Nested class Construction {
        @Test void ofVarargs() {
            var ds = Dataset.of(new Employee("A", 1, "X"));
            assertEquals(1, ds.size());
        }

        @Test void ofList() {
            var ds = Dataset.of(List.of(new Employee("A", 1, "X")));
            assertEquals(1, ds.size());
        }

        @Test void empty() {
            assertTrue(Dataset.empty().isEmpty());
            assertEquals(0, Dataset.empty().size());
        }

        @Test void concat() {
            var ds1 = Dataset.of(new Employee("A", 1, "X"));
            var ds2 = Dataset.of(new Employee("B", 2, "Y"));
            assertEquals(2, Dataset.concat(ds1, ds2).size());
        }

        @Test void immutability() {
            var list = new ArrayList<>(List.of(new Employee("A", 1, "X")));
            var ds = Dataset.of(list);
            list.add(new Employee("B", 2, "Y"));
            assertEquals(1, ds.size(), "Dataset should not be affected by external list mutation");
        }
    }

    // -----------------------------------------------------------------
    // Basic Properties
    // -----------------------------------------------------------------

    @Nested class BasicProperties {
        @Test void size() { assertEquals(5, EMPLOYEES.size()); }
        @Test void isEmpty() { assertFalse(EMPLOYEES.isEmpty()); }
        @Test void get() { assertEquals("Alice", EMPLOYEES.get(0).name()); }
        @Test void first() { assertEquals("Alice", EMPLOYEES.first().orElseThrow().name()); }
        @Test void last() { assertEquals("Eve", EMPLOYEES.last().orElseThrow().name()); }
        @Test void firstEmpty() { assertTrue(Dataset.empty().first().isEmpty()); }
        @Test void lastEmpty() { assertTrue(Dataset.empty().last().isEmpty()); }
        @Test void toList() { assertEquals(5, EMPLOYEES.toList().size()); }

        @Test void iterable() {
            int count = 0;
            for (var e : EMPLOYEES) count++;
            assertEquals(5, count);
        }
    }

    // -----------------------------------------------------------------
    // Filtering
    // -----------------------------------------------------------------

    @Nested class Filtering {
        @Test void filter() {
            var result = EMPLOYEES.filter(e -> e.age() > 28);
            assertEquals(3, result.size());
            assertTrue(result.all(e -> e.age() > 28));
        }

        @Test void whereIn() {
            var result = EMPLOYEES.whereIn(Employee::name, Set.of("Alice", "Eve"));
            assertEquals(2, result.size());
        }

        @Test void reject() {
            var result = EMPLOYEES.reject(e -> e.age() > 28);
            assertEquals(2, result.size());
            assertTrue(result.all(e -> e.age() <= 28));
        }

        @Test void dropNull() {
            record Row(String name, Integer value) {}
            var ds = Dataset.of(
                new Row("a", 1), new Row("b", null), new Row("c", 3));
            assertEquals(2, ds.dropNull(Row::value).size());
        }
    }

    // -----------------------------------------------------------------
    // Slicing
    // -----------------------------------------------------------------

    @Nested class Slicing {
        @Test void head() {
            assertEquals(2, EMPLOYEES.head(2).size());
            assertEquals("Alice", EMPLOYEES.head(2).get(0).name());
        }

        @Test void headOverflow() {
            assertEquals(5, EMPLOYEES.head(100).size());
        }

        @Test void tail() {
            assertEquals(2, EMPLOYEES.tail(2).size());
            assertEquals("Eve", EMPLOYEES.tail(2).get(1).name());
        }

        @Test void slice() {
            var result = EMPLOYEES.slice(1, 3);
            assertEquals(2, result.size());
            assertEquals("Bob", result.get(0).name());
        }

        @Test void sample() {
            var result = EMPLOYEES.sample(3, 42L);
            assertEquals(3, result.size());
        }

        @Test void sampleReproducible() {
            var a = EMPLOYEES.sample(3, 42L);
            var b = EMPLOYEES.sample(3, 42L);
            assertEquals(a.toList(), b.toList());
        }
    }

    // -----------------------------------------------------------------
    // Sorting
    // -----------------------------------------------------------------

    @Nested class Sorting {
        @Test void sortByComparable() {
            var result = EMPLOYEES.sortBy(Employee::name);
            assertEquals("Alice", result.get(0).name());
            assertEquals("Eve", result.get(4).name());
        }

        @Test void sortByInt() {
            var result = EMPLOYEES.sortByInt(Employee::age);
            assertEquals(25, result.get(0).age());
            assertEquals(35, result.get(4).age());
        }

        @Test void sortByIntDesc() {
            var result = EMPLOYEES.sortByIntDesc(Employee::age);
            assertEquals(35, result.get(0).age());
            assertEquals(25, result.get(4).age());
        }

        @Test void sortByDesc() {
            var result = EMPLOYEES.sortByDesc(Employee::name);
            assertEquals("Eve", result.get(0).name());
        }
    }

    // -----------------------------------------------------------------
    // Transformation
    // -----------------------------------------------------------------

    @Nested class Transformation {
        @Test void map() {
            record NameAge(String name, int age) {}
            var result = EMPLOYEES.map(e -> new NameAge(e.name(), e.age()));
            assertEquals(5, result.size());
            assertEquals("Alice", result.get(0).name());
        }

        @Test void flatMap() {
            record Tag(String name, String tag) {}
            var result = EMPLOYEES.head(2).flatMap(e -> List.of(
                new Tag(e.name(), "tag1"), new Tag(e.name(), "tag2")));
            assertEquals(4, result.size());
        }
    }

    // -----------------------------------------------------------------
    // Deduplication
    // -----------------------------------------------------------------

    @Nested class Deduplication {
        @Test void distinctBy() {
            var result = EMPLOYEES.distinctBy(Employee::dept);
            assertEquals(2, result.size());
            assertEquals("Alice", result.get(0).name());  // first Eng
            assertEquals("Bob", result.get(1).name());     // first Sales
        }

        @Test void distinct() {
            var ds = Dataset.of(
                new Employee("A", 1, "X"),
                new Employee("A", 1, "X"),
                new Employee("B", 2, "Y"));
            assertEquals(2, ds.distinct().size());
        }
    }

    // -----------------------------------------------------------------
    // Aggregation
    // -----------------------------------------------------------------

    @Nested class Aggregation {
        @Test void sumInt()  { assertEquals(150, EMPLOYEES.sumInt(Employee::age)); }
        @Test void meanInt() { assertEquals(30.0, EMPLOYEES.meanInt(Employee::age).orElse(0)); }
        @Test void maxInt()  { assertEquals(35, EMPLOYEES.maxInt(Employee::age).orElse(0)); }
        @Test void minInt()  { assertEquals(25, EMPLOYEES.minInt(Employee::age).orElse(0)); }

        @Test void maxBy() {
            assertEquals("Charlie", EMPLOYEES.maxBy(Employee::age).orElseThrow().name());
        }

        @Test void minBy() {
            assertEquals("Bob", EMPLOYEES.minBy(Employee::age).orElseThrow().name());
        }

        @Test void countDistinct() {
            assertEquals(2, EMPLOYEES.countDistinct(Employee::dept));
        }

        @Test void valueCounts() {
            var counts = EMPLOYEES.valueCounts(Employee::dept);
            assertEquals(3L, counts.get("Eng"));
            assertEquals(2L, counts.get("Sales"));
        }

        @Test void statsInt() {
            var stats = EMPLOYEES.statsInt(Employee::age);
            assertEquals(5, stats.getCount());
            assertEquals(150, stats.getSum());
            assertEquals(25, stats.getMin());
            assertEquals(35, stats.getMax());
        }

        @Test void nLargest() {
            var top2 = EMPLOYEES.nLargest(2, Comparator.comparingInt(Employee::age));
            assertEquals(2, top2.size());
            assertEquals(35, top2.get(0).age());
            assertEquals(32, top2.get(1).age());
        }

        @Test void nSmallest() {
            var bot2 = EMPLOYEES.nSmallest(2, Comparator.comparingInt(Employee::age));
            assertEquals(25, bot2.get(0).age());
            assertEquals(28, bot2.get(1).age());
        }
    }

    // -----------------------------------------------------------------
    // Column Extraction
    // -----------------------------------------------------------------

    @Nested class ColumnExtraction {
        @Test void column() {
            assertEquals(List.of("Alice", "Bob", "Charlie", "Diana", "Eve"),
                EMPLOYEES.column(Employee::name));
        }

        @Test void columnInt() {
            assertArrayEquals(new int[]{30, 25, 35, 28, 32},
                EMPLOYEES.columnInt(Employee::age));
        }

        @Test void indexBy() {
            var index = EMPLOYEES.indexBy(Employee::name);
            assertEquals(30, index.get("Alice").age());
            assertEquals(5, index.size());
        }
    }

    // -----------------------------------------------------------------
    // GroupBy
    // -----------------------------------------------------------------

    @Nested class GroupByTests {
        @Test void groupByKeys() {
            var grouped = EMPLOYEES.groupBy(Employee::dept);
            assertEquals(Set.of("Eng", "Sales"), grouped.keys());
        }

        @Test void groupByGet() {
            var eng = EMPLOYEES.groupBy(Employee::dept).get("Eng");
            assertEquals(3, eng.size());
        }

        @Test void counts() {
            var counts = EMPLOYEES.groupBy(Employee::dept).counts();
            assertEquals(3, counts.get("Eng"));
            assertEquals(2, counts.get("Sales"));
        }

        @Test void meanInt() {
            var means = EMPLOYEES.groupBy(Employee::dept).meanInt(Employee::age);
            assertEquals(32.333, means.get("Eng"), 0.01);
            assertEquals(26.5, means.get("Sales"), 0.01);
        }

        @Test void sumInt() {
            var sums = EMPLOYEES.groupBy(Employee::dept).sumInt(Employee::age);
            assertEquals(97, sums.get("Eng"));
            assertEquals(53, sums.get("Sales"));
        }

        @Test void aggregate() {
            record Stats(String dept, double avg, int count) {}
            var result = EMPLOYEES.groupBy(Employee::dept)
                .aggregate((dept, group) -> new Stats(
                    dept, group.meanInt(Employee::age).orElse(0), group.size()));
            assertEquals(2, result.size());
        }

        @Test void apply() {
            // Sort each group by age
            var result = EMPLOYEES.groupBy(Employee::dept)
                .apply(g -> g.sortByInt(Employee::age));
            assertEquals(5, result.size());
        }

        @Test void computePerGroup() {
            var avgByDept = EMPLOYEES.groupBy(Employee::dept)
                .computePerGroup(g -> g.meanInt(Employee::age).orElse(0));
            assertEquals(32.333, avgByDept.get("Eng"), 0.01);
        }
    }

    // -----------------------------------------------------------------
    // Joins with BiFunction (original API)
    // -----------------------------------------------------------------

    @Nested class JoinsWithMapper {
        static final Dataset<Department> DEPTS = Dataset.of(
            new Department("Eng", "SF"),
            new Department("Sales", "NYC")
        );

        @Test void innerJoinCustom() {
            record Result(String name, String location) {}
            var result = EMPLOYEES.innerJoin(DEPTS,
                Employee::dept, Department::dept,
                (e, d) -> new Result(e.name(), d.location()));
            assertEquals(5, result.size());
            assertEquals("SF", result.get(0).location());
        }

        @Test void leftJoinCustom() {
            var deptsPartial = Dataset.of(new Department("Eng", "SF"));
            record Result(String name, String location) {}
            var result = EMPLOYEES.leftJoin(deptsPartial,
                Employee::dept, Department::dept,
                (e, d) -> new Result(e.name(), d != null ? d.location() : null));
            assertEquals(5, result.size());
            assertNull(result.filter(r -> r.name().equals("Bob")).get(0).location());
        }
    }

    // -----------------------------------------------------------------
    // Joins returning Pair
    // -----------------------------------------------------------------

    @Nested class PairJoins {
        static final Dataset<Department> DEPTS = Dataset.of(
            new Department("Eng", "SF"),
            new Department("Sales", "NYC")
        );

        @Test void innerJoinPair() {
            var result = EMPLOYEES.innerJoin(DEPTS, Employee::dept, Department::dept);
            assertEquals(5, result.size());
            assertEquals("Alice", result.get(0).left().name());
            assertEquals("SF", result.get(0).right().location());
        }

        @Test void leftJoinPair() {
            var deptsPartial = Dataset.of(new Department("Eng", "SF"));
            var result = EMPLOYEES.leftJoin(deptsPartial, Employee::dept, Department::dept);
            assertEquals(5, result.size());

            var bob = result.filter(p -> p.left().name().equals("Bob")).get(0);
            assertNull(bob.right());
            assertFalse(bob.hasRight());

            var alice = result.filter(p -> p.left().name().equals("Alice")).get(0);
            assertNotNull(alice.right());
            assertTrue(alice.hasRight());
        }

        @Test void rightJoinPair() {
            var deptsWithHR = Dataset.of(
                new Department("Eng", "SF"),
                new Department("Sales", "NYC"),
                new Department("HR", "Chicago")
            );
            var result = EMPLOYEES.rightJoin(deptsWithHR, Employee::dept, Department::dept);

            // HR has no employees → left is null
            var hrRows = result.filter(p -> p.right().dept().equals("HR"));
            assertEquals(1, hrRows.size());
            assertNull(hrRows.get(0).left());
            assertFalse(hrRows.get(0).hasLeft());

            // Eng has 3 employees
            assertEquals(3, result.filter(p -> p.right().dept().equals("Eng")).size());
        }

        @Test void crossJoin() {
            record Color(String name) {}
            record Size(String label) {}
            var colors = Dataset.of(new Color("R"), new Color("B"));
            var sizes = Dataset.of(new Size("S"), new Size("M"), new Size("L"));
            var result = colors.crossJoin(sizes);
            assertEquals(6, result.size());
            assertEquals("R", result.get(0).left().name());
            assertEquals("S", result.get(0).right().label());
        }

        @Test void pairChaining() {
            // Filter and map on joined data without custom records
            var names = EMPLOYEES.innerJoin(DEPTS, Employee::dept, Department::dept)
                .filter(p -> p.left().age() > 28)
                .filter(p -> p.right().location().equals("SF"))
                .column(p -> p.left().name());
            assertEquals(List.of("Alice", "Charlie", "Eve"), names);
        }
    }

    // -----------------------------------------------------------------
    // Pair utilities
    // -----------------------------------------------------------------

    @Nested class PairTests {
        @Test void mapBoth() {
            var p = new Pair<>("hello", 42);
            assertEquals("hello=42", p.map((l, r) -> l + "=" + r));
        }

        @Test void mapLeft() {
            var p = new Pair<>("hello", 42);
            assertEquals("HELLO", p.mapLeft(String::toUpperCase).left());
            assertEquals(42, p.mapLeft(String::toUpperCase).right());
        }

        @Test void mapRight() {
            var p = new Pair<>("hello", 42);
            assertEquals(84, p.mapRight(r -> r * 2).right());
        }


        @Test void positionalAccessors() {
            var p = new Pair<>("a", "b");
            assertEquals("a", p.first());
            assertEquals("b", p.second());
        }

        @Test void toStringTest() {
            assertEquals("(hello, 42)", new Pair<>("hello", 42).toString());
        }

        @Test void equalsAndHashCode() {
            var p1 = new Pair<>("a", 1);
            var p2 = new Pair<>("a", 1);
            assertEquals(p1, p2);
            assertEquals(p1.hashCode(), p2.hashCode());
        }
    }


    // -----------------------------------------------------------------
    // Window operations
    // -----------------------------------------------------------------

    @Nested class WindowOps {
        @Test void cumSumInt() {
            assertEquals(List.of(30, 55, 90, 118, 150),
                EMPLOYEES.cumSumInt(Employee::age));
        }

        @Test void rollingMean() {
            var result = EMPLOYEES.rollingMeanInt(Employee::age, 3);
            assertEquals(5, result.size());
            assertTrue(result.get(0).isEmpty());
            assertTrue(result.get(1).isEmpty());
            assertEquals(30.0, result.get(2).orElse(0), 0.01); // (30+25+35)/3
        }

        @Test void shift() {
            var shifted = EMPLOYEES.shift(Employee::name, 1);
            assertNull(shifted.get(0));
            assertEquals("Alice", shifted.get(1));
            assertEquals("Bob", shifted.get(2));
        }
    }

    // -----------------------------------------------------------------
    // Any / All / Count
    // -----------------------------------------------------------------

    @Nested class Predicates {
        @Test void any()   { assertTrue(EMPLOYEES.any(e -> e.age() > 34)); }
        @Test void anyNo() { assertFalse(EMPLOYEES.any(e -> e.age() > 40)); }
        @Test void all()   { assertTrue(EMPLOYEES.all(e -> e.age() > 20)); }
        @Test void allNo() { assertFalse(EMPLOYEES.all(e -> e.age() > 30)); }
        @Test void count() { assertEquals(3, EMPLOYEES.count(e -> e.age() > 28)); }
    }

    // -----------------------------------------------------------------
    // Pipe
    // -----------------------------------------------------------------

    @Test void pipe() {
        int total = EMPLOYEES.pipe(ds ->
            ds.filter(e -> e.age() < 30).sumInt(Employee::age));
        assertEquals(53, total);
    }

    // -----------------------------------------------------------------
    // Multi-key joins with CompositeKey
    // -----------------------------------------------------------------

    @Nested class MultiKeyJoins {
        
        record EmployeeExt(String name, int age, String dept, String location) {}
        record DepartmentExt(String dept, String location, String manager) {}
        
        static final Dataset<EmployeeExt> EMP_EXT = Dataset.of(
            new EmployeeExt("Alice",   30, "Eng", "SF"),
            new EmployeeExt("Bob",     25, "Sales", "NYC"),
            new EmployeeExt("Charlie", 35, "Eng", "NYC"),
            new EmployeeExt("Diana",   28, "Sales", "SF")
        );
        
        static final Dataset<DepartmentExt> DEPT_EXT = Dataset.of(
            new DepartmentExt("Eng", "SF", "Tech Lead SF"),
            new DepartmentExt("Eng", "NYC", "Tech Lead NYC"),
            new DepartmentExt("Sales", "SF", "Sales Manager SF"),
            new DepartmentExt("Sales", "LA", "Sales Manager LA") // no matching employee
        );

        @Test void compositeKeyEquality() {
            var key1 = key("Eng", "SF");
            var key2 = key("Eng", "SF");
            var key3 = key("Eng", "NYC");
            
            assertEquals(key1, key2);
            assertNotEquals(key1, key3);
            assertEquals(key1.hashCode(), key2.hashCode());
        }

        @Test void innerJoinMulti() {
            var joined = EMP_EXT.innerJoinMulti(DEPT_EXT,
                e -> key(e.dept(), e.location()),
                d -> key(d.dept(), d.location()));
            
            assertEquals(3, joined.size()); // Alice, Charlie, Diana match
            
            var alice = joined.stream()
                .filter(p -> p.left().name().equals("Alice"))
                .findFirst().orElseThrow();
            assertEquals("Tech Lead SF", alice.right().manager());
            
            var charlie = joined.stream()
                .filter(p -> p.left().name().equals("Charlie"))
                .findFirst().orElseThrow();
            assertEquals("Tech Lead NYC", charlie.right().manager());
        }

        @Test void leftJoinMulti() {
            var joined = EMP_EXT.leftJoinMulti(DEPT_EXT,
                e -> key(e.dept(), e.location()),
                d -> key(d.dept(), d.location()));
            
            assertEquals(4, joined.size()); // All employees present
            
            var bob = joined.stream()
                .filter(p -> p.left().name().equals("Bob"))
                .findFirst().orElseThrow();
            assertNull(bob.right()); // Bob is Sales-NYC, no matching dept
        }

        @Test void innerJoin2Convenience() {
            var joined = EMP_EXT.innerJoin2(DEPT_EXT,
                EmployeeExt::dept, EmployeeExt::location,
                DepartmentExt::dept, DepartmentExt::location);
            
            assertEquals(3, joined.size());
            
            // Verify we get the same results as innerJoinMulti
            var names = joined.stream()
                .map(p -> p.left().name())
                .sorted()
                .toList();
            assertEquals(List.of("Alice", "Charlie", "Diana"), names);
        }

        @Test void leftJoin2Convenience() {
            var joined = EMP_EXT.leftJoin2(DEPT_EXT,
                EmployeeExt::dept, EmployeeExt::location,
                DepartmentExt::dept, DepartmentExt::location);
            
            assertEquals(4, joined.size());
            
            long nullCount = joined.stream()
                .filter(p -> p.right() == null)
                .count();
            assertEquals(1, nullCount); // Bob has no match
        }

        @Test void innerJoin3KeysConvenience() {
            record Product(String category, String brand, String region, int price) {}
            record Inventory(String category, String brand, String region, int stock) {}
            
            var products = Dataset.of(
                new Product("Electronics", "Apple", "US", 999),
                new Product("Electronics", "Samsung", "US", 799),
                new Product("Clothing", "Nike", "EU", 120)
            );
            
            var inventory = Dataset.of(
                new Inventory("Electronics", "Apple", "US", 50),
                new Inventory("Electronics", "Samsung", "US", 25),
                new Inventory("Clothing", "Adidas", "EU", 100) // different brand
            );
            
            var joined = products.innerJoin3Keys(inventory,
                Product::category, Product::brand, Product::region,
                Inventory::category, Inventory::brand, Inventory::region);
            
            assertEquals(2, joined.size()); // Apple and Samsung match, Nike doesn't
            
            var appleJoin = joined.stream()
                .filter(p -> p.left().brand().equals("Apple"))
                .findFirst().orElseThrow();
            assertEquals(50, appleJoin.right().stock());
        }

        @Test void compositeKeyWithVariousTypes() {
            var key = key("string", 42, true, 3.14);
            assertEquals(4, key.size());
            assertEquals("string", key.get(0));
            assertEquals(42, key.get(1));
            assertEquals(true, key.get(2));
            assertEquals(3.14, key.get(3));
        }

        @Test void compositeKeyToString() {
            var key = key("Eng", "SF");
            assertEquals("CompositeKey[Eng, SF]", key.toString());
        }

        @Test void compositeKeyEmptyThrows() {
            assertThrows(IllegalArgumentException.class, () -> key());
        }

        // ---------------------------------------------------------------
        // Fluent join API tests using CompositeKey.on()
        // ---------------------------------------------------------------

        @Test void innerJoinOnFluent() {
            var joined = EMP_EXT.innerJoinOn(DEPT_EXT,
                on(EmployeeExt::dept, EmployeeExt::location),
                on(DepartmentExt::dept, DepartmentExt::location));
            
            assertEquals(3, joined.size()); // Alice, Charlie, Diana match
            
            // Verify same results as innerJoinMulti
            var names = joined.stream()
                .map(p -> p.left().name())
                .sorted()
                .toList();
            assertEquals(List.of("Alice", "Charlie", "Diana"), names);
        }

        @Test void leftJoinOnFluent() {
            var joined = EMP_EXT.leftJoinOn(DEPT_EXT,
                on(EmployeeExt::dept, EmployeeExt::location),
                on(DepartmentExt::dept, DepartmentExt::location));
            
            assertEquals(4, joined.size()); // All employees present
            
            long nullCount = joined.stream()
                .filter(p -> p.right() == null)
                .count();
            assertEquals(1, nullCount); // Bob has no match
        }

        @Test void rightJoinOnFluent() {
            var joined = EMP_EXT.rightJoinOn(DEPT_EXT,
                on(EmployeeExt::dept, EmployeeExt::location),
                on(DepartmentExt::dept, DepartmentExt::location));
            
            assertEquals(4, joined.size()); // All departments present
            
            // Sales-LA department has no matching employee
            var salesLA = joined.stream()
                .filter(p -> p.right() != null && 
                            p.right().dept().equals("Sales") && 
                            p.right().location().equals("LA"))
                .findFirst().orElseThrow();
            assertNull(salesLA.left());
        }

        @Test void fluentJoinWithThreeKeys() {
            record Product(String category, String brand, String region, int price) {}
            record Inventory(String category, String brand, String region, int stock) {}
            
            var products = Dataset.of(
                new Product("Electronics", "Apple", "US", 999),
                new Product("Electronics", "Samsung", "US", 799),
                new Product("Clothing", "Nike", "EU", 120)
            );
            
            var inventory = Dataset.of(
                new Inventory("Electronics", "Apple", "US", 50),
                new Inventory("Electronics", "Samsung", "US", 25),
                new Inventory("Clothing", "Adidas", "EU", 100)
            );
            
            var joined = products.innerJoinOn(inventory,
                on(Product::category, Product::brand, Product::region),
                on(Inventory::category, Inventory::brand, Inventory::region));
            
            assertEquals(2, joined.size()); // Apple and Samsung match, Nike doesn't
            
            var appleJoin = joined.stream()
                .filter(p -> p.left().brand().equals("Apple"))
                .findFirst().orElseThrow();
            assertEquals(50, appleJoin.right().stock());
        }

        @Test void fluentJoinWithFourKeys() {
            record OrderExt(String dept, String location, String quarter, String product) {}
            record BudgetExt(String dept, String location, String quarter, String product, int amount) {}
            
            var orders = Dataset.of(
                new OrderExt("Eng", "SF", "Q1", "Laptop"),
                new OrderExt("Sales", "NYC", "Q2", "Phone")
            );
            
            var budgets = Dataset.of(
                new BudgetExt("Eng", "SF", "Q1", "Laptop", 5000),
                new BudgetExt("Sales", "LA", "Q2", "Phone", 3000) // different location
            );
            
            var joined = orders.innerJoinOn(budgets,
                on(OrderExt::dept, OrderExt::location, OrderExt::quarter, OrderExt::product),
                on(BudgetExt::dept, BudgetExt::location, BudgetExt::quarter, BudgetExt::product));
            
            assertEquals(1, joined.size()); // Only Eng-SF-Q1-Laptop matches
            assertEquals(5000, joined.get(0).right().amount());
        }
    }

    // -----------------------------------------------------------------
    // toString
    // -----------------------------------------------------------------

    @Test void toStringSmall() {
        var s = EMPLOYEES.head(2).toString();
        assertTrue(s.contains("size=2"));
        assertTrue(s.contains("Alice"));
    }

    @Test void toStringEmpty() {
        assertEquals("Dataset(empty)", Dataset.empty().toString());
    }
}
