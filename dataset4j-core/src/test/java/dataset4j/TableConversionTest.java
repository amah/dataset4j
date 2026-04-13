package dataset4j;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TableConversionTest {

    record Employee(String name, int age, String dept) {}
    record Person(String name, double score, boolean active) {}
    record WithDate(String name, LocalDate birth) {}

    // -----------------------------------------------------------------
    // Dataset → Table
    // -----------------------------------------------------------------

    @Test
    void datasetToTable() {
        Dataset<Employee> ds = Dataset.of(
                new Employee("Alice", 30, "Eng"),
                new Employee("Bob", 25, "Sales")
        );

        Table table = ds.toTable();

        assertEquals(2, table.size());
        assertEquals(List.of("name", "age", "dept"), table.columns());
        assertEquals("Alice", table.value(0, "name"));
        assertEquals(30, table.cellValue(0, "age").asInt());
        assertEquals("Eng", table.value(0, "dept"));
    }

    @Test
    void datasetToTablePreservesTypes() {
        Dataset<Person> ds = Dataset.of(
                new Person("Alice", 95.5, true)
        );

        Table table = ds.toTable();

        assertEquals(ValueType.STRING, table.cellValue(0, "name").type());
        assertEquals(ValueType.NUMBER, table.cellValue(0, "score").type());
        assertEquals(ValueType.BOOLEAN, table.cellValue(0, "active").type());
    }

    @Test
    void emptyDatasetToTable() {
        Dataset<Employee> ds = Dataset.empty();
        Table table = ds.toTable();
        assertTrue(table.isEmpty());
    }

    @Test
    void datasetToTableWithDates() {
        Dataset<WithDate> ds = Dataset.of(
                new WithDate("Alice", LocalDate.of(1990, 5, 15))
        );

        Table table = ds.toTable();
        assertEquals(ValueType.DATE, table.cellValue(0, "birth").type());
        assertEquals(LocalDate.of(1990, 5, 15), table.cellValue(0, "birth").value());
    }

    // -----------------------------------------------------------------
    // Table → Dataset
    // -----------------------------------------------------------------

    @Test
    void tableToDataset() {
        Table table = Table.builder()
                .columns("name", "age", "dept")
                .row("Alice", 30, "Eng")
                .row("Bob", 25, "Sales")
                .build();

        Dataset<Employee> ds = table.toDataset(Employee.class);

        assertEquals(2, ds.size());
        assertEquals("Alice", ds.get(0).name());
        assertEquals(30, ds.get(0).age());
        assertEquals("Eng", ds.get(0).dept());
        assertEquals("Bob", ds.get(1).name());
        assertEquals(25, ds.get(1).age());
    }

    @Test
    void tableToDatasetWithCoercion() {
        // age stored as double (e.g. from Excel), should coerce to int
        Table table = Table.builder()
                .columns("name", "age", "dept")
                .rowCells(
                        CellValue.ofString("Alice"),
                        CellValue.ofNumber(30.0),
                        CellValue.ofString("Eng")
                )
                .build();

        Dataset<Employee> ds = table.toDataset(Employee.class);
        assertEquals(30, ds.get(0).age());
    }

    @Test
    void tableToDatasetWithStringCoercion() {
        // All values as strings (e.g. from CSV), should coerce to int/double/boolean
        Table table = Table.builder()
                .columns("name", "score", "active")
                .rowCells(
                        CellValue.ofString("Alice"),
                        CellValue.ofString("95.5"),
                        CellValue.ofString("true")
                )
                .build();

        Dataset<Person> ds = table.toDataset(Person.class);
        assertEquals("Alice", ds.get(0).name());
        assertEquals(95.5, ds.get(0).score(), 0.001);
        assertTrue(ds.get(0).active());
    }

    @Test
    void tableToDatasetWithBlanks() {
        Table table = Table.builder()
                .columns("name", "age", "dept")
                .row("Alice", null, null)
                .build();

        Dataset<Employee> ds = table.toDataset(Employee.class);
        assertEquals("Alice", ds.get(0).name());
        assertEquals(0, ds.get(0).age()); // int default
        assertNull(ds.get(0).dept()); // String default is null
    }

    @Test
    void tableToDatasetRejectsNonRecord() {
        Table table = Table.builder().columns("x").row(1).build();
        assertThrows(IllegalArgumentException.class, () -> table.toDataset(String.class));
    }

    // -----------------------------------------------------------------
    // Round-trip: Dataset → Table → Dataset
    // -----------------------------------------------------------------

    @Test
    void roundTrip() {
        Dataset<Employee> original = Dataset.of(
                new Employee("Alice", 30, "Eng"),
                new Employee("Bob", 25, "Sales"),
                new Employee("Charlie", 35, "Eng")
        );

        Table table = original.toTable();
        Dataset<Employee> roundTripped = table.toDataset(Employee.class);

        assertEquals(original.size(), roundTripped.size());
        for (int i = 0; i < original.size(); i++) {
            assertEquals(original.get(i).name(), roundTripped.get(i).name());
            assertEquals(original.get(i).age(), roundTripped.get(i).age());
            assertEquals(original.get(i).dept(), roundTripped.get(i).dept());
        }
    }

    @Test
    void roundTripWithTransformation() {
        Dataset<Employee> original = Dataset.of(
                new Employee("Alice", 30, "Eng"),
                new Employee("Bob", 25, "Sales"),
                new Employee("Charlie", 35, "Eng")
        );

        // Convert to Table, transform, convert back
        Table table = original.toTable()
                .filter(row -> row.get("dept").asString().equals("Eng"))
                .sortBy("age");

        Dataset<Employee> result = table.toDataset(Employee.class);

        assertEquals(2, result.size());
        assertEquals("Alice", result.get(0).name());
        assertEquals("Charlie", result.get(1).name());
    }

    // -----------------------------------------------------------------
    // Table.fromDataset()
    // -----------------------------------------------------------------

    @Test
    void fromDataset() {
        Dataset<Employee> ds = Dataset.of(
                new Employee("Alice", 30, "Eng")
        );

        Table table = Table.fromDataset(ds);
        assertEquals(1, table.size());
        assertEquals("Alice", table.value(0, "name"));
    }
}
