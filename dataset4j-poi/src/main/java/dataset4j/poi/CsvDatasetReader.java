package dataset4j.poi;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVParserBuilder;
import com.opencsv.exceptions.CsvException;
import dataset4j.CellValue;
import dataset4j.Table;
import dataset4j.ValueType;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Reads CSV files into an untyped {@link Table}.
 *
 * <p>All values are read as {@link ValueType#STRING} since CSV has no type metadata.
 * Use the typed column accessors on {@link Table} (e.g. {@code intColumn()}, {@code doubleColumn()})
 * to coerce values as needed.
 *
 * <p>Example:
 * <pre>{@code
 * Table table = CsvDatasetReader
 *     .fromFile("data.csv")
 *     .separator(',')
 *     .readTable();
 * }</pre>
 */
public class CsvDatasetReader {

    private final String filePath;
    private boolean hasHeaders = true;
    private char separator = ',';
    private char quoteChar = '"';
    private char escapeChar = '\\';
    private int skipLines = 0;

    private CsvDatasetReader(String filePath) {
        this.filePath = filePath;
    }

    /**
     * Create a reader for the given CSV file.
     * @param filePath path to the CSV file
     * @return new reader instance
     */
    public static CsvDatasetReader fromFile(String filePath) {
        Path path = Paths.get(filePath).normalize();
        if (path.toString().contains("..")) {
            throw new SecurityException("Path traversal detected in file path: " + filePath);
        }
        return new CsvDatasetReader(filePath);
    }

    /** Whether the first row contains headers. Default: true. */
    public CsvDatasetReader headers(boolean hasHeaders) {
        this.hasHeaders = hasHeaders;
        return this;
    }

    /** Alias for {@link #headers(boolean)}. */
    public CsvDatasetReader hasHeaders(boolean hasHeaders) {
        return headers(hasHeaders);
    }

    /** Set the separator character. Default: comma. */
    public CsvDatasetReader separator(char separator) {
        this.separator = separator;
        return this;
    }

    /** Set the quote character. Default: double-quote. */
    public CsvDatasetReader quoteChar(char quoteChar) {
        this.quoteChar = quoteChar;
        return this;
    }

    /** Set the escape character. Default: backslash. */
    public CsvDatasetReader escapeChar(char escapeChar) {
        this.escapeChar = escapeChar;
        return this;
    }

    /** Number of lines to skip before reading (before headers). Default: 0. */
    public CsvDatasetReader skipLines(int skipLines) {
        this.skipLines = skipLines;
        return this;
    }

    /**
     * Read the CSV file into a {@link Table}.
     *
     * <p>All values are stored as {@link ValueType#STRING} with no format metadata.
     *
     * @return a Table containing the CSV data
     * @throws IOException if the file cannot be read
     */
    public Table readTable() throws IOException {
        try (CSVReader reader = new CSVReaderBuilder(new FileReader(filePath))
                .withCSVParser(new CSVParserBuilder()
                        .withSeparator(separator)
                        .withQuoteChar(quoteChar)
                        .withEscapeChar(escapeChar)
                        .build())
                .withSkipLines(skipLines)
                .build()) {

            List<String[]> allLines = reader.readAll();
            if (allLines.isEmpty()) {
                return Table.empty();
            }

            List<String> columnNames;
            int dataStart;

            if (hasHeaders) {
                String[] headerLine = allLines.get(0);
                columnNames = new ArrayList<>();
                for (int i = 0; i < headerLine.length; i++) {
                    String name = headerLine[i].trim();
                    columnNames.add(name.isEmpty() ? "Column" + (i + 1) : name);
                }
                dataStart = 1;
            } else {
                int colCount = allLines.get(0).length;
                columnNames = new ArrayList<>();
                for (int i = 0; i < colCount; i++) {
                    columnNames.add("Column" + (i + 1));
                }
                dataStart = 0;
            }

            List<Map<String, CellValue>> rows = new ArrayList<>();
            for (int i = dataStart; i < allLines.size(); i++) {
                String[] line = allLines.get(i);
                Map<String, CellValue> row = new LinkedHashMap<>();
                for (int c = 0; c < columnNames.size(); c++) {
                    String value = c < line.length ? line[c] : "";
                    if (value.isEmpty()) {
                        row.put(columnNames.get(c), CellValue.blank());
                    } else {
                        row.put(columnNames.get(c), CellValue.ofString(value));
                    }
                }
                rows.add(row);
            }

            return Table.of(columnNames, rows);

        } catch (CsvException e) {
            throw new IOException("Failed to parse CSV file: " + e.getMessage(), e);
        }
    }
}
