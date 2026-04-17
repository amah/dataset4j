package dataset4j.parquet;

import dataset4j.CellValue;
import dataset4j.Dataset;
import dataset4j.DatasetReadException;
import dataset4j.Table;
import dataset4j.ValueType;
import dataset4j.annotations.AnnotationProcessor;
import dataset4j.annotations.ColumnMetadata;
import dataset4j.annotations.FormatProvider;
import org.apache.parquet.format.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Lightweight Parquet reader producing Datasets of Java records.
 *
 * <p>Reads spec-compliant Parquet files written by {@link ParquetDatasetWriter}. Supports
 * PLAIN value encoding and RLE/Bit-Packed Hybrid definition-level encoding (bit-width 1).
 * Files using DICTIONARY encoding (parquet-mr's default) or other advanced encodings
 * are not currently supported.
 *
 * <p>Example:
 * <pre>{@code
 * Dataset<Employee> employees = ParquetDatasetReader
 *     .fromFile("employees.parquet")
 *     .readAs(Employee.class);
 * }</pre>
 */
public class ParquetDatasetReader {

    private static final long DEFAULT_MAX_FILE_SIZE = 1L * 1024 * 1024 * 1024; // 1GB

    private final Path filePath;
    private long maxFileSize = DEFAULT_MAX_FILE_SIZE;

    // Configurable default values applied when a column is missing from the file or
    // when its decoded value is null. Per-field overrides take priority over per-type.
    private final Map<Class<?>, Object> typeDefaults = new LinkedHashMap<>();
    private final Map<String, Object> fieldDefaults = new LinkedHashMap<>();

    private ParquetDatasetReader(String filePath) {
        this.filePath = Paths.get(filePath);
    }

    public static ParquetDatasetReader fromFile(String filePath) {
        Path path = Paths.get(filePath).normalize();
        if (path.toString().contains("..")) {
            throw new SecurityException("Path traversal detected in file path: " + filePath);
        }
        return new ParquetDatasetReader(filePath);
    }

    /** Set maximum allowed file size in bytes (default: 1GB). */
    public ParquetDatasetReader maxFileSize(long maxBytes) {
        if (maxBytes <= 0) throw new IllegalArgumentException("maxFileSize must be positive");
        this.maxFileSize = maxBytes;
        return this;
    }

    /**
     * Set a default value for all fields of the given type when the column is missing from
     * the file or its decoded value is null. Mirrors {@code ExcelDatasetReader.defaultValue}.
     */
    public ParquetDatasetReader defaultValue(Class<?> type, Object value) {
        this.typeDefaults.put(type, value);
        return this;
    }

    /**
     * Set a default value for a specific record field. Per-field defaults take priority over
     * per-type defaults and over {@code @DataColumn(defaultValue=...)}.
     */
    public ParquetDatasetReader defaultValue(String fieldName, Object value) {
        this.fieldDefaults.put(fieldName, value);
        return this;
    }

    public boolean canRead() {
        try {
            return filePath.toFile().exists() && filePath.toFile().canRead();
        } catch (Exception e) {
            return false;
        }
    }

    /** Read the file's schema info without decoding row data. */
    public ParquetSchemaInfo getSchemaInfo() throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r");
             FileChannel ch = raf.getChannel()) {
            FileMetaData fmd = readFooter(ch);
            return new ParquetSchemaInfo(toPublicSchema(fmd));
        }
    }

    /** Read the Parquet file into a Dataset of records. */
    public <T> Dataset<T> readAs(Class<T> recordClass) throws IOException {
        List<T> result = new ArrayList<>();
        processRowGroups(recordClass, result::add);
        return Dataset.of(result);
    }

    /**
     * Stream records one at a time without materializing the full Dataset in memory.
     * Processes row groups sequentially, releasing each row group's decoded column arrays
     * before moving to the next. Use this instead of {@link #readAs} when the file is too
     * large to hold in memory all at once.
     *
     * <p>Example:
     * <pre>{@code
     * ParquetDatasetReader.fromFile("huge.parquet")
     *     .forEach(Employee.class, employee -> {
     *         process(employee);
     *     });
     * }</pre>
     */
    public <T> void forEach(Class<T> recordClass, Consumer<T> consumer) throws IOException {
        processRowGroups(recordClass, consumer);
    }

    /**
     * Shared row-group iteration: decode each row group's columns, instantiate records row-by-row,
     * and deliver each record to {@code handler}. Column arrays for one row group are released
     * before the next row group is decoded, keeping peak memory proportional to the largest single
     * row group rather than the whole file.
     */
    private <T> void processRowGroups(Class<T> recordClass, Consumer<T> handler) throws IOException {
        long fileSize = Files.size(filePath);
        if (fileSize > maxFileSize) {
            throw new IOException("File too large: " + fileSize + " bytes (max " + maxFileSize + " bytes). "
                    + "Use maxFileSize() to increase the limit.");
        }
        if (!recordClass.isRecord()) {
            throw new IllegalArgumentException("Class must be a record: " + recordClass.getName());
        }

        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r");
             FileChannel ch = raf.getChannel()) {

            FileMetaData fmd = readFooter(ch);
            Map<String, SchemaElement> schemaByName = new HashMap<>();
            for (SchemaElement se : fmd.getSchema()) {
                if (se.isSetType()) schemaByName.put(se.getName(), se);
            }

            RecordComponent[] components = recordClass.getRecordComponents();
            Class<?>[] paramTypes = new Class<?>[components.length];
            for (int i = 0; i < components.length; i++) paramTypes[i] = components[i].getType();

            // Build a map: Parquet column name → record component index. Honors @DataColumn(name=...)
            // via ColumnMetadata.getEffectiveColumnName(); falls back to the bare component name so
            // records without annotations still resolve when names match exactly.
            // Also build componentIdx → ColumnMetadata so the conversion step can honor dateFormat
            // and defaultValue from @DataColumn.
            Map<String, Integer> parquetColToComponentIdx = new HashMap<>();
            ColumnMetadata[] componentMetas = new ColumnMetadata[components.length];
            try {
                List<ColumnMetadata> colMetas = AnnotationProcessor.extractColumns(recordClass);
                for (ColumnMetadata cm : colMetas) {
                    for (int i = 0; i < components.length; i++) {
                        if (components[i].getName().equals(cm.getFieldName())) {
                            parquetColToComponentIdx.put(cm.getEffectiveColumnName(), i);
                            parquetColToComponentIdx.putIfAbsent(cm.getFieldName(), i);
                            componentMetas[i] = cm;
                            break;
                        }
                    }
                }
            } catch (RuntimeException ignored) {
                // Records without @DataColumn fall back to component-name matching.
            }
            for (int i = 0; i < components.length; i++) {
                parquetColToComponentIdx.putIfAbsent(components[i].getName(), i);
            }

            Constructor<T> ctor;
            try {
                ctor = recordClass.getDeclaredConstructor(paramTypes);
                ctor.setAccessible(true);
            } catch (NoSuchMethodException e) {
                throw new IOException("No canonical constructor on " + recordClass.getName(), e);
            }

            int rowOffset = 0;
            for (RowGroup rg : fmd.getRow_groups()) {
                int numRows = (int) rg.getNum_rows();
                // columns[componentIdx] = decoded column data for that record component (null if
                // the file has no matching column). Each ColumnChunkData uses primitive arrays
                // where possible to avoid the ~4× boxing blow-up of Object[] for numeric columns.
                ColumnChunkData[] columns = new ColumnChunkData[components.length];
                for (ColumnChunk cc : rg.getColumns()) {
                    ColumnMetaData cmd = cc.getMeta_data();
                    String colName = cmd.getPath_in_schema().get(0);
                    Integer compIdx = parquetColToComponentIdx.get(colName);
                    if (compIdx == null) continue;
                    SchemaElement se = schemaByName.get(colName);
                    columns[compIdx] = readColumnChunk(ch, cmd, se, numRows);
                }

                for (int row = 0; row < numRows; row++) {
                    Object[] args = new Object[components.length];
                    for (int i = 0; i < components.length; i++) {
                        Object raw = columns[i] == null ? null : columns[i].valueAt(row);
                        Class<?> targetType = components[i].getType();
                        ColumnMetadata cm = componentMetas[i];
                        try {
                            args[i] = convertToTarget(raw, targetType, cm);
                        } catch (RuntimeException e) {
                            throw DatasetReadException.builder()
                                    .row(rowOffset + row)
                                    .recordClass(recordClass)
                                    .fieldName(components[i].getName())
                                    .fieldTypeName(targetType.getSimpleName())
                                    .rawValue(raw == null ? null : raw.toString())
                                    .parseMessage(e.getMessage())
                                    .cause(e)
                                    .build();
                        }
                    }
                    T record;
                    try {
                        record = ctor.newInstance(args);
                    } catch (ReflectiveOperationException e) {
                        throw DatasetReadException.builder()
                                .row(rowOffset + row)
                                .recordClass(recordClass)
                                .parseMessage("Failed to instantiate record: " + e.getMessage())
                                .cause(e)
                                .build();
                    }
                    handler.accept(record);
                }
                rowOffset += numRows;
            }
        }
    }

    /**
     * Read the Parquet file into an untyped {@link Table}, preserving type information
     * from the Parquet schema.
     *
     * @return a Table with CellValues derived from Parquet physical/logical types
     * @throws IOException if the file cannot be read
     */
    public Table readTable() throws IOException {
        long fileSize = Files.size(filePath);
        if (fileSize > maxFileSize) {
            throw new IOException("File too large: " + fileSize + " bytes (max " + maxFileSize + " bytes). "
                    + "Use maxFileSize() to increase the limit.");
        }

        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r");
             FileChannel ch = raf.getChannel()) {

            FileMetaData fmd = readFooter(ch);

            // Build ordered list of column names and schema elements
            List<String> columnNames = new ArrayList<>();
            Map<String, SchemaElement> schemaByName = new LinkedHashMap<>();
            for (SchemaElement se : fmd.getSchema()) {
                if (se.isSetType()) {
                    columnNames.add(se.getName());
                    schemaByName.put(se.getName(), se);
                }
            }

            if (columnNames.isEmpty()) {
                return Table.empty();
            }

            List<Map<String, CellValue>> allRows = new ArrayList<>();

            for (RowGroup rg : fmd.getRow_groups()) {
                int numRows = (int) rg.getNum_rows();

                // Decode each column chunk
                Map<String, ColumnChunkData> columnData = new LinkedHashMap<>();
                Map<String, SchemaElement> columnSchemas = new LinkedHashMap<>();
                for (ColumnChunk cc : rg.getColumns()) {
                    ColumnMetaData cmd = cc.getMeta_data();
                    String colName = cmd.getPath_in_schema().get(0);
                    SchemaElement se = schemaByName.get(colName);
                    if (se == null) continue;
                    columnData.put(colName, readColumnChunk(ch, cmd, se, numRows));
                    columnSchemas.put(colName, se);
                }

                // Build rows
                for (int row = 0; row < numRows; row++) {
                    Map<String, CellValue> rowMap = new LinkedHashMap<>();
                    for (String colName : columnNames) {
                        ColumnChunkData data = columnData.get(colName);
                        Object raw = data == null ? null : data.valueAt(row);
                        SchemaElement se = columnSchemas.get(colName);
                        rowMap.put(colName, toCellValue(raw, se));
                    }
                    allRows.add(rowMap);
                }
            }

            return Table.of(columnNames, allRows);
        }
    }

    /**
     * Convert a raw decoded Parquet value + schema element into a CellValue
     * with the appropriate ValueType.
     */
    private static CellValue toCellValue(Object raw, SchemaElement se) {
        if (raw == null) {
            return CellValue.blank();
        }

        if (se == null) {
            // No schema info — infer from value
            return Table.wrapValue(raw);
        }

        // Determine ValueType from Parquet schema
        Type physicalType = se.getType();

        if (raw instanceof Boolean) {
            return CellValue.of(raw, ValueType.BOOLEAN);
        }

        if (raw instanceof LocalDate) {
            return CellValue.of(raw, ValueType.DATE);
        }

        if (raw instanceof LocalDateTime) {
            return CellValue.of(raw, ValueType.DATETIME);
        }

        if (raw instanceof BigDecimal) {
            String format = null;
            if (se.isSetScale() && se.isSetPrecision()) {
                format = "DECIMAL(" + se.getPrecision() + "," + se.getScale() + ")";
            }
            return CellValue.of(raw, ValueType.NUMBER, format);
        }

        if (raw instanceof Number) {
            return CellValue.of(raw, ValueType.NUMBER);
        }

        if (raw instanceof String) {
            return CellValue.of(raw, ValueType.STRING);
        }

        // Fallback
        return CellValue.of(raw, ValueType.STRING);
    }

    // ---------- Footer ----------

    private FileMetaData readFooter(FileChannel ch) throws IOException {
        long size = ch.size();
        if (size < 12) throw new IOException("File too small to be a Parquet file: " + size + " bytes");

        ByteBuffer trailer = ByteBuffer.allocate(8);
        ch.position(size - 8);
        readFully(ch, trailer);
        trailer.flip();
        trailer.order(ByteOrder.LITTLE_ENDIAN);
        int footerLen = trailer.getInt();
        byte[] magic = new byte[4];
        trailer.get(magic);
        if (!Arrays.equals(magic, ParquetIo.MAGIC)) {
            throw new IOException("Not a Parquet file (bad trailing magic): "
                    + new String(magic, StandardCharsets.US_ASCII));
        }
        if (footerLen <= 0 || footerLen > size - 12) {
            throw new IOException("Invalid footer length: " + footerLen + " (file size " + size + ")");
        }

        ByteBuffer footerBuf = ByteBuffer.allocate(footerLen);
        ch.position(size - 8 - footerLen);
        readFully(ch, footerBuf);
        return Util.readFileMetaData(new ByteArrayInputStream(footerBuf.array()));
    }

    // ---------- Column chunk decoding ----------

    /**
     * Decode one column chunk into a {@link ColumnChunkData}. Uses a primitive array
     * (int[]/long[]/float[]/double[]/boolean[]) for numeric/boolean columns without logical
     * type conversion, avoiding the ~4× boxing overhead of holding an {@code Object[numRows]}
     * of {@code Integer}/{@code Long}/{@code Double} for the whole row group. Columns that
     * need a reference-typed representation (strings, decimals, dates, byte[]) fall back to
     * {@code Object[]}.
     */
    private ColumnChunkData readColumnChunk(FileChannel ch, ColumnMetaData cmd, SchemaElement se, int numRows)
            throws IOException {
        long firstPageOffset = cmd.isSetDictionary_page_offset() && cmd.getDictionary_page_offset() > 0
                ? cmd.getDictionary_page_offset()
                : cmd.getData_page_offset();
        int totalCompressed = (int) cmd.getTotal_compressed_size();
        ParquetCompressionCodec codec = fromThriftCodec(cmd.getCodec());
        boolean optional = se.getRepetition_type() == FieldRepetitionType.OPTIONAL;

        ByteBuffer chunkBuf = ByteBuffer.allocate(totalCompressed);
        ch.position(firstPageOffset);
        readFully(ch, chunkBuf);
        ByteArrayInputStream bais = new ByteArrayInputStream(chunkBuf.array());

        ColumnChunkData.Kind kind = storageKind(se);
        // Destination arrays — only one is non-null, matching kind.
        int[] ints = kind == ColumnChunkData.Kind.INT ? new int[numRows] : null;
        long[] longs = kind == ColumnChunkData.Kind.LONG ? new long[numRows] : null;
        float[] floats = kind == ColumnChunkData.Kind.FLOAT ? new float[numRows] : null;
        double[] doubles = kind == ColumnChunkData.Kind.DOUBLE ? new double[numRows] : null;
        boolean[] booleans = kind == ColumnChunkData.Kind.BOOLEAN ? new boolean[numRows] : null;
        Object[] objects = kind == ColumnChunkData.Kind.OBJECT ? new Object[numRows] : null;
        BitSet nullMask = null;

        // Dictionary storage (same kind as output, if a dictionary page is present)
        int[] dictInts = null;
        long[] dictLongs = null;
        float[] dictFloats = null;
        double[] dictDoubles = null;
        boolean[] dictBooleans = null;
        Object[] dictObjects = null;

        int rowIdx = 0;
        while (rowIdx < numRows && bais.available() > 0) {
            PageHeader ph = Util.readPageHeader(bais);
            int compressedPageSize = ph.getCompressed_page_size();
            int uncompressedPageSize = ph.getUncompressed_page_size();
            byte[] compressed = bais.readNBytes(compressedPageSize);
            if (compressed.length != compressedPageSize) {
                throw new IOException("Truncated page payload: expected " + compressedPageSize
                        + " bytes, got " + compressed.length);
            }
            byte[] uncompressed = ParquetIo.decompress(compressed, codec, uncompressedPageSize);
            ByteBuffer pageBuf = ByteBuffer.wrap(uncompressed);

            PageType pt = ph.getType();
            if (pt == PageType.DICTIONARY_PAGE) {
                DictionaryPageHeader dictHeader = ph.getDictionary_page_header();
                int numEntries = dictHeader.getNum_values();
                switch (kind) {
                    case INT -> dictInts = decodePlainInts(pageBuf, numEntries);
                    case LONG -> dictLongs = decodePlainLongs(pageBuf, numEntries);
                    case FLOAT -> dictFloats = decodePlainFloats(pageBuf, numEntries);
                    case DOUBLE -> dictDoubles = decodePlainDoubles(pageBuf, numEntries);
                    case BOOLEAN -> dictBooleans = ParquetIo.decodePlainBooleans(pageBuf, numEntries);
                    case OBJECT -> dictObjects = decodePlainObjects(pageBuf, se, numEntries);
                }
                continue;
            }
            if (pt != PageType.DATA_PAGE) {
                throw new IOException("Unsupported page type: " + pt
                        + " (DATA_PAGE_V1 and DICTIONARY_PAGE supported)");
            }

            DataPageHeader dph = ph.getData_page_header();
            int pageNumValues = dph.getNum_values();

            int[] defLevels = null;
            int nonNullCount;
            if (optional) {
                defLevels = ParquetIo.decodeDefLevels(pageBuf, 1, pageNumValues);
                int nn = 0;
                for (int v : defLevels) if (v == 1) nn++;
                nonNullCount = nn;
            } else {
                nonNullCount = pageNumValues;
            }

            // Decode the non-null values into a tmp buffer of the right primitive type,
            // then scatter into the destination array guided by defLevels. The tmp buffer
            // is sized to nonNullCount and becomes garbage after this page.
            int writable = Math.min(pageNumValues, numRows - rowIdx);
            Encoding enc = dph.getEncoding();

            if (enc == Encoding.PLAIN) {
                switch (kind) {
                    case INT -> {
                        int[] tmp = decodePlainInts(pageBuf, nonNullCount);
                        nullMask = scatterInts(tmp, ints, defLevels, writable, rowIdx, nullMask, numRows);
                    }
                    case LONG -> {
                        long[] tmp = decodePlainLongs(pageBuf, nonNullCount);
                        nullMask = scatterLongs(tmp, longs, defLevels, writable, rowIdx, nullMask, numRows);
                    }
                    case FLOAT -> {
                        float[] tmp = decodePlainFloats(pageBuf, nonNullCount);
                        nullMask = scatterFloats(tmp, floats, defLevels, writable, rowIdx, nullMask, numRows);
                    }
                    case DOUBLE -> {
                        double[] tmp = decodePlainDoubles(pageBuf, nonNullCount);
                        nullMask = scatterDoubles(tmp, doubles, defLevels, writable, rowIdx, nullMask, numRows);
                    }
                    case BOOLEAN -> {
                        boolean[] tmp = ParquetIo.decodePlainBooleans(pageBuf, nonNullCount);
                        nullMask = scatterBooleans(tmp, booleans, defLevels, writable, rowIdx, nullMask, numRows);
                    }
                    case OBJECT -> {
                        Object[] tmp = decodePlainObjects(pageBuf, se, nonNullCount);
                        nullMask = scatterObjects(tmp, objects, defLevels, writable, rowIdx, nullMask, numRows);
                    }
                }
                rowIdx += writable;
            } else if (enc == Encoding.RLE_DICTIONARY || enc == Encoding.PLAIN_DICTIONARY) {
                int bitWidth = pageBuf.get() & 0xFF;
                int[] indices = ParquetIo.decodeHybrid(pageBuf, bitWidth, nonNullCount, pageBuf.limit());
                int di = 0;
                for (int i = 0; i < writable; i++) {
                    boolean isNull = defLevels != null && defLevels[i] != 1;
                    if (isNull) {
                        if (nullMask == null) nullMask = new BitSet(numRows);
                        nullMask.set(rowIdx);
                    } else {
                        int idx = indices[di++];
                        switch (kind) {
                            case INT -> {
                                if (dictInts == null) throw new IOException("Dictionary-encoded data page for column "
                                        + se.getName() + " has no preceding dictionary page");
                                ints[rowIdx] = dictInts[idx];
                            }
                            case LONG -> {
                                if (dictLongs == null) throw new IOException("Dictionary-encoded data page for column "
                                        + se.getName() + " has no preceding dictionary page");
                                longs[rowIdx] = dictLongs[idx];
                            }
                            case FLOAT -> {
                                if (dictFloats == null) throw new IOException("Dictionary-encoded data page for column "
                                        + se.getName() + " has no preceding dictionary page");
                                floats[rowIdx] = dictFloats[idx];
                            }
                            case DOUBLE -> {
                                if (dictDoubles == null) throw new IOException("Dictionary-encoded data page for column "
                                        + se.getName() + " has no preceding dictionary page");
                                doubles[rowIdx] = dictDoubles[idx];
                            }
                            case BOOLEAN -> {
                                if (dictBooleans == null) throw new IOException("Dictionary-encoded data page for column "
                                        + se.getName() + " has no preceding dictionary page");
                                booleans[rowIdx] = dictBooleans[idx];
                            }
                            case OBJECT -> {
                                if (dictObjects == null) throw new IOException("Dictionary-encoded data page for column "
                                        + se.getName() + " has no preceding dictionary page");
                                objects[rowIdx] = dictObjects[idx];
                            }
                        }
                    }
                    rowIdx++;
                }
            } else {
                throw new IOException("Unsupported value encoding: " + enc
                        + " for column " + se.getName()
                        + " (supported: PLAIN, RLE_DICTIONARY, PLAIN_DICTIONARY)");
            }
        }

        return switch (kind) {
            case INT -> ColumnChunkData.forInts(ints, nullMask);
            case LONG -> ColumnChunkData.forLongs(longs, nullMask);
            case FLOAT -> ColumnChunkData.forFloats(floats, nullMask);
            case DOUBLE -> ColumnChunkData.forDoubles(doubles, nullMask);
            case BOOLEAN -> ColumnChunkData.forBooleans(booleans, nullMask);
            case OBJECT -> ColumnChunkData.forObjects(objects, nullMask);
        };
    }

    /**
     * Pick the primitive storage kind for a column, or OBJECT when a logical-type conversion
     * (DATE, DECIMAL, STRING, byte[]) needs a reference-typed representation.
     */
    private static ColumnChunkData.Kind storageKind(SchemaElement se) {
        Type t = se.getType();
        boolean isDecimal = isLogicalDecimal(se);
        boolean isDate = isLogicalDate(se);
        return switch (t) {
            case BOOLEAN -> ColumnChunkData.Kind.BOOLEAN;
            case FLOAT -> ColumnChunkData.Kind.FLOAT;
            case DOUBLE -> ColumnChunkData.Kind.DOUBLE;
            case INT32 -> (isDate || isDecimal) ? ColumnChunkData.Kind.OBJECT : ColumnChunkData.Kind.INT;
            case INT64 -> isDecimal ? ColumnChunkData.Kind.OBJECT : ColumnChunkData.Kind.LONG;
            default -> ColumnChunkData.Kind.OBJECT; // BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96
        };
    }

    private static int[] decodePlainInts(ByteBuffer buf, int n) {
        ByteBuffer le = buf.slice().order(ByteOrder.LITTLE_ENDIAN);
        int[] out = new int[n];
        for (int i = 0; i < n; i++) out[i] = le.getInt();
        buf.position(buf.position() + n * 4);
        return out;
    }

    private static long[] decodePlainLongs(ByteBuffer buf, int n) {
        ByteBuffer le = buf.slice().order(ByteOrder.LITTLE_ENDIAN);
        long[] out = new long[n];
        for (int i = 0; i < n; i++) out[i] = le.getLong();
        buf.position(buf.position() + n * 8);
        return out;
    }

    private static float[] decodePlainFloats(ByteBuffer buf, int n) {
        ByteBuffer le = buf.slice().order(ByteOrder.LITTLE_ENDIAN);
        float[] out = new float[n];
        for (int i = 0; i < n; i++) out[i] = le.getFloat();
        buf.position(buf.position() + n * 4);
        return out;
    }

    private static double[] decodePlainDoubles(ByteBuffer buf, int n) {
        ByteBuffer le = buf.slice().order(ByteOrder.LITTLE_ENDIAN);
        double[] out = new double[n];
        for (int i = 0; i < n; i++) out[i] = le.getDouble();
        buf.position(buf.position() + n * 8);
        return out;
    }

    /**
     * Decode PLAIN-encoded values for columns that need reference-typed representation:
     * BYTE_ARRAY (String / byte[] / BigDecimal), FIXED_LEN_BYTE_ARRAY (byte[] / BigDecimal),
     * and INT32/INT64 with DATE or DECIMAL logical types.
     */
    private static Object[] decodePlainObjects(ByteBuffer buf, SchemaElement se, int n) {
        Type t = se.getType();
        Object[] out = new Object[n];
        boolean isDecimal = isLogicalDecimal(se);
        int decimalScale = isDecimal ? se.getScale() : 0;
        switch (t) {
            case INT32: {
                ByteBuffer le = buf.slice().order(ByteOrder.LITTLE_ENDIAN);
                boolean isDate = isLogicalDate(se);
                for (int i = 0; i < n; i++) {
                    int v = le.getInt();
                    if (isDate) out[i] = LocalDate.ofEpochDay(v);
                    else if (isDecimal) out[i] = BigDecimal.valueOf((long) v, decimalScale);
                    else out[i] = Integer.valueOf(v);
                }
                buf.position(buf.position() + n * 4);
                return out;
            }
            case INT64: {
                ByteBuffer le = buf.slice().order(ByteOrder.LITTLE_ENDIAN);
                for (int i = 0; i < n; i++) {
                    long v = le.getLong();
                    if (isDecimal) out[i] = BigDecimal.valueOf(v, decimalScale);
                    else out[i] = Long.valueOf(v);
                }
                buf.position(buf.position() + n * 8);
                return out;
            }
            case BYTE_ARRAY: {
                boolean isString = isLogicalString(se);
                ByteBuffer le = buf.order(ByteOrder.LITTLE_ENDIAN);
                for (int i = 0; i < n; i++) {
                    int len = le.getInt();
                    byte[] bytes = new byte[len];
                    le.get(bytes);
                    if (isDecimal) {
                        out[i] = new BigDecimal(new BigInteger(bytes), decimalScale);
                    } else if (isString) {
                        out[i] = new String(bytes, StandardCharsets.UTF_8);
                    } else {
                        out[i] = new String(bytes, StandardCharsets.UTF_8);
                    }
                }
                return out;
            }
            case FIXED_LEN_BYTE_ARRAY: {
                int len = se.getType_length();
                for (int i = 0; i < n; i++) {
                    byte[] bytes = new byte[len];
                    buf.get(bytes);
                    if (isDecimal) {
                        out[i] = new BigDecimal(new BigInteger(bytes), decimalScale);
                    } else {
                        out[i] = bytes;
                    }
                }
                return out;
            }
            default:
                throw new UnsupportedOperationException("Physical type not supported for object path: " + t);
        }
    }

    // Scatter helpers: write `tmp` (of size nonNullCount) into `dest` at [rowIdx, rowIdx+writable),
    // guided by defLevels. Marks null rows in the returned (lazily-allocated) BitSet.

    private static BitSet scatterInts(int[] tmp, int[] dest, int[] defLevels, int writable,
                                       int rowIdx, BitSet nullMask, int numRows) {
        if (defLevels == null) {
            System.arraycopy(tmp, 0, dest, rowIdx, writable);
            return nullMask;
        }
        int di = 0;
        for (int i = 0; i < writable; i++) {
            if (defLevels[i] == 1) dest[rowIdx + i] = tmp[di++];
            else { if (nullMask == null) nullMask = new BitSet(numRows); nullMask.set(rowIdx + i); }
        }
        return nullMask;
    }

    private static BitSet scatterLongs(long[] tmp, long[] dest, int[] defLevels, int writable,
                                        int rowIdx, BitSet nullMask, int numRows) {
        if (defLevels == null) {
            System.arraycopy(tmp, 0, dest, rowIdx, writable);
            return nullMask;
        }
        int di = 0;
        for (int i = 0; i < writable; i++) {
            if (defLevels[i] == 1) dest[rowIdx + i] = tmp[di++];
            else { if (nullMask == null) nullMask = new BitSet(numRows); nullMask.set(rowIdx + i); }
        }
        return nullMask;
    }

    private static BitSet scatterFloats(float[] tmp, float[] dest, int[] defLevels, int writable,
                                         int rowIdx, BitSet nullMask, int numRows) {
        if (defLevels == null) {
            System.arraycopy(tmp, 0, dest, rowIdx, writable);
            return nullMask;
        }
        int di = 0;
        for (int i = 0; i < writable; i++) {
            if (defLevels[i] == 1) dest[rowIdx + i] = tmp[di++];
            else { if (nullMask == null) nullMask = new BitSet(numRows); nullMask.set(rowIdx + i); }
        }
        return nullMask;
    }

    private static BitSet scatterDoubles(double[] tmp, double[] dest, int[] defLevels, int writable,
                                          int rowIdx, BitSet nullMask, int numRows) {
        if (defLevels == null) {
            System.arraycopy(tmp, 0, dest, rowIdx, writable);
            return nullMask;
        }
        int di = 0;
        for (int i = 0; i < writable; i++) {
            if (defLevels[i] == 1) dest[rowIdx + i] = tmp[di++];
            else { if (nullMask == null) nullMask = new BitSet(numRows); nullMask.set(rowIdx + i); }
        }
        return nullMask;
    }

    private static BitSet scatterBooleans(boolean[] tmp, boolean[] dest, int[] defLevels, int writable,
                                           int rowIdx, BitSet nullMask, int numRows) {
        if (defLevels == null) {
            System.arraycopy(tmp, 0, dest, rowIdx, writable);
            return nullMask;
        }
        int di = 0;
        for (int i = 0; i < writable; i++) {
            if (defLevels[i] == 1) dest[rowIdx + i] = tmp[di++];
            else { if (nullMask == null) nullMask = new BitSet(numRows); nullMask.set(rowIdx + i); }
        }
        return nullMask;
    }

    private static BitSet scatterObjects(Object[] tmp, Object[] dest, int[] defLevels, int writable,
                                          int rowIdx, BitSet nullMask, int numRows) {
        if (defLevels == null) {
            System.arraycopy(tmp, 0, dest, rowIdx, writable);
            return nullMask;
        }
        int di = 0;
        for (int i = 0; i < writable; i++) {
            if (defLevels[i] == 1) dest[rowIdx + i] = tmp[di++];
            else { if (nullMask == null) nullMask = new BitSet(numRows); nullMask.set(rowIdx + i); }
        }
        return nullMask;
    }

    /**
     * Decoded column data. Numeric/boolean columns use a primitive array to avoid holding a
     * boxed Integer/Long/Double/Float/Boolean per row for the whole row group — the main source
     * of heap pressure in the old {@code Object[]} path. Columns with logical type conversions
     * (String, BigDecimal, LocalDate, byte[]) use {@code Object[]}.
     *
     * <p>Nulls are tracked via a lazily-allocated {@link BitSet} so REQUIRED columns and nullable
     * columns with no observed nulls pay no extra storage.
     */
    private static final class ColumnChunkData {
        enum Kind { INT, LONG, FLOAT, DOUBLE, BOOLEAN, OBJECT }

        private final Kind kind;
        private final int[] ints;
        private final long[] longs;
        private final float[] floats;
        private final double[] doubles;
        private final boolean[] booleans;
        private final Object[] objects;
        private final BitSet nullMask;

        private ColumnChunkData(Kind kind, int[] ints, long[] longs, float[] floats,
                                 double[] doubles, boolean[] booleans, Object[] objects,
                                 BitSet nullMask) {
            this.kind = kind;
            this.ints = ints;
            this.longs = longs;
            this.floats = floats;
            this.doubles = doubles;
            this.booleans = booleans;
            this.objects = objects;
            this.nullMask = nullMask;
        }

        static ColumnChunkData forInts(int[] v, BitSet nullMask) {
            return new ColumnChunkData(Kind.INT, v, null, null, null, null, null, nullMask);
        }
        static ColumnChunkData forLongs(long[] v, BitSet nullMask) {
            return new ColumnChunkData(Kind.LONG, null, v, null, null, null, null, nullMask);
        }
        static ColumnChunkData forFloats(float[] v, BitSet nullMask) {
            return new ColumnChunkData(Kind.FLOAT, null, null, v, null, null, null, nullMask);
        }
        static ColumnChunkData forDoubles(double[] v, BitSet nullMask) {
            return new ColumnChunkData(Kind.DOUBLE, null, null, null, v, null, null, nullMask);
        }
        static ColumnChunkData forBooleans(boolean[] v, BitSet nullMask) {
            return new ColumnChunkData(Kind.BOOLEAN, null, null, null, null, v, null, nullMask);
        }
        static ColumnChunkData forObjects(Object[] v, BitSet nullMask) {
            return new ColumnChunkData(Kind.OBJECT, null, null, null, null, null, v, nullMask);
        }

        /**
         * Returns the row's value as Object, boxing primitives on the fly. The box lives only
         * as long as the caller needs it — HotSpot's escape analysis can often elide it when
         * the caller immediately passes it to {@code Constructor.newInstance} and the ctor
         * unboxes into a primitive field.
         */
        Object valueAt(int row) {
            if (nullMask != null && nullMask.get(row)) return null;
            return switch (kind) {
                case INT -> ints[row];
                case LONG -> longs[row];
                case FLOAT -> floats[row];
                case DOUBLE -> doubles[row];
                case BOOLEAN -> booleans[row];
                case OBJECT -> objects[row];
            };
        }
    }

    private static boolean isLogicalString(SchemaElement se) {
        if (se.isSetLogicalType() && se.getLogicalType().isSetSTRING()) return true;
        return se.isSetConverted_type() && se.getConverted_type() == ConvertedType.UTF8;
    }

    private static boolean isLogicalDecimal(SchemaElement se) {
        if (se.isSetLogicalType() && se.getLogicalType().isSetDECIMAL()) return true;
        return se.isSetConverted_type() && se.getConverted_type() == ConvertedType.DECIMAL;
    }

    private static boolean isLogicalDate(SchemaElement se) {
        if (se.isSetLogicalType() && se.getLogicalType().isSetDATE()) return true;
        return se.isSetConverted_type() && se.getConverted_type() == ConvertedType.DATE;
    }

    // ---------- Type conversion to record component types ----------

    /**
     * Convert a decoded raw value (as produced by {@link #decodePlainValues}) to the record
     * component's declared type. Null values trigger the default-value chain
     * ({@link #fieldDefaults} → {@link #typeDefaults} → {@code @DataColumn(defaultValue)} →
     * built-in default). String values are parsed via {@link FormatProvider#parseValue} which
     * honors {@code @DataColumn(dateFormat=...)} and supports the full Excel-reader type set.
     */
    private Object convertToTarget(Object raw, Class<?> target, ColumnMetadata cm) {
        if (raw == null) return resolveDefault(target, cm);
        if (target.isInstance(raw)) return raw;

        // String → typed: route through FormatProvider so dateFormat / alternativeDateFormats /
        // numeric formats from @DataColumn are honored. This is the path for BYTE_ARRAY+STRING
        // columns being read into LocalDateTime / ZonedDateTime / OffsetDateTime / LocalDate
        // (with a custom dateFormat) / BigDecimal / Date / etc.
        if (raw instanceof String s && target != String.class) {
            if (cm != null) {
                Object parsed = FormatProvider.parseValue(s, cm);
                if (target.isInstance(parsed)) return parsed;
                // FormatProvider returned a String fallback or wrong type — fall through to manual.
            }
            return parseStringManually(s, target);
        }

        // Direct conversions for non-String raws
        if (target == String.class) return raw.toString();
        if (target == BigDecimal.class && raw instanceof Number n) {
            return new BigDecimal(n.toString());
        }
        if (target == LocalDate.class && raw instanceof Integer i) {
            return LocalDate.ofEpochDay(i);
        }
        if (raw instanceof Number n) {
            if (target == Integer.class || target == int.class) return n.intValue();
            if (target == Long.class || target == long.class) return n.longValue();
            if (target == Float.class || target == float.class) return n.floatValue();
            if (target == Double.class || target == double.class) return n.doubleValue();
        }
        throw new IllegalStateException("Cannot convert " + raw.getClass().getName()
                + " to " + target.getName());
    }

    /** Manual string parsing for the case where we have no ColumnMetadata to drive FormatProvider. */
    private static Object parseStringManually(String s, Class<?> target) {
        if (target == String.class) return s;
        if (target == Integer.class || target == int.class) return Integer.parseInt(s);
        if (target == Long.class || target == long.class) return Long.parseLong(s);
        if (target == Double.class || target == double.class) return Double.parseDouble(s);
        if (target == Float.class || target == float.class) return Float.parseFloat(s);
        if (target == Boolean.class || target == boolean.class) return Boolean.parseBoolean(s);
        if (target == BigDecimal.class) return new BigDecimal(s);
        if (target == LocalDate.class) return LocalDate.parse(s);
        if (target == LocalDateTime.class) return LocalDateTime.parse(s);
        if (target == ZonedDateTime.class) return ZonedDateTime.parse(s);
        if (target == OffsetDateTime.class) return OffsetDateTime.parse(s);
        if (target == Date.class) {
            return Date.from(LocalDateTime.parse(s).atZone(java.time.ZoneId.systemDefault()).toInstant());
        }
        throw new IllegalStateException("Cannot parse string '" + s + "' to " + target.getName());
    }

    /**
     * Resolve the value used when a column is missing from the file or its decoded value is null.
     * Priority chain mirrors {@link dataset4j.poi.ExcelDatasetReader}:
     * <ol>
     *   <li>Per-field override via {@link #defaultValue(String, Object)}</li>
     *   <li>Per-type override via {@link #defaultValue(Class, Object)}</li>
     *   <li>{@code @DataColumn(defaultValue="...")} annotation, parsed via FormatProvider</li>
     *   <li>Built-in default (zero/false/null)</li>
     * </ol>
     */
    private Object resolveDefault(Class<?> targetType, ColumnMetadata cm) {
        if (cm != null && fieldDefaults.containsKey(cm.getFieldName())) {
            return fieldDefaults.get(cm.getFieldName());
        }
        if (typeDefaults.containsKey(targetType)) {
            return typeDefaults.get(targetType);
        }
        if (cm != null && cm.getDefaultValue() != null && !cm.getDefaultValue().isEmpty()) {
            try {
                return FormatProvider.parseValue(cm.getDefaultValue(), cm);
            } catch (RuntimeException ignored) {
                return parseStringManually(cm.getDefaultValue(), targetType);
            }
        }
        return builtInDefault(targetType);
    }

    private static Object builtInDefault(Class<?> type) {
        // Only return non-null for true primitive types (records can't accept null primitives).
        // Wrapper types and BigDecimal return null so existing null-round-trip behavior is preserved.
        if (type == int.class) return 0;
        if (type == long.class) return 0L;
        if (type == double.class) return 0.0;
        if (type == float.class) return 0f;
        if (type == boolean.class) return false;
        return null;
    }

    // ---------- Schema translation ----------

    private static ParquetSchema toPublicSchema(FileMetaData fmd) {
        ParquetSchema schema = new ParquetSchema();
        for (SchemaElement se : fmd.getSchema()) {
            if (!se.isSetType()) continue;
            ParquetDataType dt = switch (se.getType()) {
                case BOOLEAN -> ParquetDataType.BOOLEAN;
                case INT32 -> ParquetDataType.INT32;
                case INT64 -> ParquetDataType.INT64;
                case INT96 -> ParquetDataType.INT96;
                case FLOAT -> ParquetDataType.FLOAT;
                case DOUBLE -> ParquetDataType.DOUBLE;
                case BYTE_ARRAY -> ParquetDataType.BYTE_ARRAY;
                case FIXED_LEN_BYTE_ARRAY -> ParquetDataType.FIXED_LEN_BYTE_ARRAY;
            };
            boolean required = se.isSetRepetition_type()
                    && se.getRepetition_type() == FieldRepetitionType.REQUIRED;
            schema.addColumn(new ParquetColumn(se.getName(), dt, required));
        }
        return schema;
    }

    private static ParquetCompressionCodec fromThriftCodec(CompressionCodec c) {
        return switch (c) {
            case UNCOMPRESSED -> ParquetCompressionCodec.UNCOMPRESSED;
            case SNAPPY -> ParquetCompressionCodec.SNAPPY;
            case GZIP -> ParquetCompressionCodec.GZIP;
            case LZ4, LZ4_RAW -> ParquetCompressionCodec.LZ4;
            case BROTLI -> ParquetCompressionCodec.BROTLI;
            default -> throw new UnsupportedOperationException("Unsupported codec: " + c);
        };
    }

    private static void readFully(FileChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int n = ch.read(buf);
            if (n < 0) throw new IOException("Unexpected EOF");
        }
    }
}
