package dataset4j.parquet;

import dataset4j.Dataset;
import dataset4j.annotations.DataColumn;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that {@link ParquetDatasetReader} can read Parquet files written by external tools
 * (pandas / pyarrow), which use {@code RLE_DICTIONARY} encoding by default and store
 * {@code DECIMAL} columns as {@code FIXED_LEN_BYTE_ARRAY}.
 *
 * <p>The test reads a fixture at {@code /tmp/parquet-fixtures/sample.parquet} when present.
 * If the fixture is missing, the test is skipped (so CI without Python won't fail).
 *
 * <p><b>Generating the fixture (Python snippet):</b>
 *
 * <pre>{@code
 * # Setup (do once):
 * #   mkdir -p /tmp/parquet-fixtures
 * #   python3 -m venv /tmp/parquet-fixtures/.venv
 * #   /tmp/parquet-fixtures/.venv/bin/pip install pyarrow pandas
 * #
 * # Then run:
 * #   /tmp/parquet-fixtures/.venv/bin/python /tmp/parquet-fixtures/make_sample.py
 * #
 * # make_sample.py contents:
 *
 * import sys
 * from datetime import date
 * from decimal import Decimal
 *
 * import pandas as pd
 * import pyarrow as pa
 * import pyarrow.parquet as pq
 *
 * rows = []
 * for i in range(50):
 *     rows.append({
 *         "id": i + 1,
 *         "name": f"Employee {i + 1}",
 *         "email": f"emp{i + 1}@company.com" if i % 7 != 0 else None,
 *         "active": (i % 2 == 0),
 *         "salary": Decimal("50000.00") + Decimal(i) * Decimal("250.50"),
 *         "birth_date": date(1980 + (i % 30), (i % 12) + 1, (i % 28) + 1),
 *     })
 * df = pd.DataFrame(rows)
 *
 * schema = pa.schema([
 *     ("id",         pa.int32()),
 *     ("name",       pa.string()),
 *     ("email",      pa.string()),
 *     ("active",     pa.bool_()),
 *     ("salary",     pa.decimal128(12, 2)),
 *     ("birth_date", pa.date32()),
 * ])
 * table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
 *
 * # Default settings exercise: SNAPPY codec, RLE_DICTIONARY values for byte/int columns,
 * # DICTIONARY_PAGE preceding DATA_PAGE_V1, BYTE_ARRAY strings with STRING logical type,
 * # FIXED_LEN_BYTE_ARRAY for the decimal128 column, INT32+DATE for the date column.
 * pq.write_table(table, "/tmp/parquet-fixtures/sample.parquet", compression="snappy")
 *
 * # Inspect the encodings produced (sanity check / debugging your own files):
 * pf = pq.ParquetFile("/tmp/parquet-fixtures/sample.parquet")
 * print(pf.schema_arrow)
 * for c in range(pf.metadata.num_columns):
 *     col = pf.metadata.row_group(0).column(c)
 *     print(c, col.path_in_schema, col.physical_type, col.encodings, col.compression)
 * }</pre>
 *
 * <p>To diagnose your own failing file, run the inspection block above against it and look at:
 * <ul>
 *   <li><b>encodings</b> — only {@code PLAIN}, {@code RLE}, {@code RLE_DICTIONARY},
 *       {@code PLAIN_DICTIONARY} are supported. Anything else (e.g. {@code DELTA_BINARY_PACKED},
 *       {@code DELTA_BYTE_ARRAY}, {@code BYTE_STREAM_SPLIT}) is currently rejected.</li>
 *   <li><b>physical_type</b> — {@code INT96} is not supported (legacy Impala/Spark timestamps).</li>
 * </ul>
 */
class ParquetExternalReadTest {

    @TempDir
    Path tempDir;

    public record SampleEmployee(
        @DataColumn(name = "id")         Integer id,
        @DataColumn(name = "name")       String name,
        @DataColumn(name = "email")      String email,
        @DataColumn(name = "active")     Boolean active,
        @DataColumn(name = "salary")     BigDecimal salary,
        @DataColumn(name = "birth_date") LocalDate birthDate
    ) {}

    @Test
    void shouldReadPyarrowGeneratedFile() throws IOException {
        Path fixture = Path.of("/tmp/parquet-fixtures/sample.parquet");
        if (!java.nio.file.Files.exists(fixture)) {
            // Fixture not present — skip rather than fail. See class Javadoc for how to generate it.
            System.out.println("Skipping: " + fixture + " not found");
            return;
        }

        Dataset<SampleEmployee> ds = ParquetDatasetReader
                .fromFile(fixture.toString())
                .readAs(SampleEmployee.class);

        assertEquals(50, ds.size());
        List<SampleEmployee> rows = ds.toList();

        // Spot-check the first row (i=0): email is null because 0 % 7 == 0.
        SampleEmployee first = rows.get(0);
        assertEquals(1, first.id());
        assertEquals("Employee 1", first.name());
        assertNull(first.email());
        assertTrue(first.active());
        assertEquals(0, new BigDecimal("50000.00").compareTo(first.salary()));
        assertEquals(LocalDate.of(1980, 1, 1), first.birthDate());

        // 8 of 50 emails should be null (every 7th row starting at 0).
        long emailNonNull = rows.stream().filter(e -> e.email() != null).count();
        assertEquals(42, emailNonNull);

        // birth_date is non-nullable in the source — every row should have a date.
        long birthNonNull = rows.stream().filter(e -> e.birthDate() != null).count();
        assertEquals(50, birthNonNull);

        // active is the inverse boolean pattern (i % 2 == 0).
        for (int i = 0; i < rows.size(); i++) {
            assertEquals(i % 2 == 0, rows.get(i).active(), "row " + i);
        }
    }
}
