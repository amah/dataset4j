package dataset4j.parquet;

import dataset4j.Dataset;
import dataset4j.annotations.AnnotationProcessor;
import dataset4j.annotations.ColumnMetadata;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
            Map<String, Integer> parquetColToComponentIdx = new HashMap<>();
            try {
                List<ColumnMetadata> colMetas = AnnotationProcessor.extractColumns(recordClass);
                for (ColumnMetadata cm : colMetas) {
                    for (int i = 0; i < components.length; i++) {
                        if (components[i].getName().equals(cm.getFieldName())) {
                            parquetColToComponentIdx.put(cm.getEffectiveColumnName(), i);
                            // Also register the bare field name as a fallback for files whose
                            // schema uses the field name rather than the annotated column name.
                            parquetColToComponentIdx.putIfAbsent(cm.getFieldName(), i);
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

            List<T> result = new ArrayList<>();
            for (RowGroup rg : fmd.getRow_groups()) {
                int numRows = (int) rg.getNum_rows();
                // values[componentIdx] = decoded column array for that record component (may be null
                // if the file has no matching column).
                Object[][] values = new Object[components.length][];
                for (ColumnChunk cc : rg.getColumns()) {
                    ColumnMetaData cmd = cc.getMeta_data();
                    String colName = cmd.getPath_in_schema().get(0);
                    Integer compIdx = parquetColToComponentIdx.get(colName);
                    if (compIdx == null) continue; // file column not present in record — skip
                    SchemaElement se = schemaByName.get(colName);
                    values[compIdx] = readColumnChunk(ch, cmd, se, numRows);
                }

                for (int row = 0; row < numRows; row++) {
                    Object[] args = new Object[components.length];
                    for (int i = 0; i < components.length; i++) {
                        Object raw = values[i] == null ? null : values[i][row];
                        args[i] = convertToTarget(raw, components[i].getType());
                    }
                    try {
                        result.add(ctor.newInstance(args));
                    } catch (ReflectiveOperationException e) {
                        throw new IOException("Failed to instantiate record " + recordClass.getName(), e);
                    }
                }
            }
            return Dataset.of(result);
        }
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

    private Object[] readColumnChunk(FileChannel ch, ColumnMetaData cmd, SchemaElement se, int numRows)
            throws IOException {
        // The chunk starts at the dictionary page if one is present, otherwise at the first data page.
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

        Object[] dictionary = null;
        Object[] out = new Object[numRows];
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
                // Dictionary entries are PLAIN-encoded values of the column's physical type.
                dictionary = decodePlainValues(pageBuf, se, numEntries);
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

            Object[] decoded;
            Encoding enc = dph.getEncoding();
            if (enc == Encoding.PLAIN) {
                decoded = decodePlainValues(pageBuf, se, nonNullCount);
            } else if (enc == Encoding.RLE_DICTIONARY || enc == Encoding.PLAIN_DICTIONARY) {
                if (dictionary == null) {
                    throw new IOException("Dictionary-encoded data page for column "
                            + se.getName() + " has no preceding dictionary page");
                }
                int bitWidth = pageBuf.get() & 0xFF;
                int[] indices = ParquetIo.decodeHybrid(pageBuf, bitWidth, nonNullCount, pageBuf.limit());
                decoded = new Object[nonNullCount];
                for (int i = 0; i < nonNullCount; i++) decoded[i] = dictionary[indices[i]];
            } else {
                throw new IOException("Unsupported value encoding: " + enc
                        + " for column " + se.getName()
                        + " (supported: PLAIN, RLE_DICTIONARY, PLAIN_DICTIONARY)");
            }

            // Weave decoded values + nulls into the output for this page
            if (defLevels == null) {
                int writable = Math.min(pageNumValues, numRows - rowIdx);
                System.arraycopy(decoded, 0, out, rowIdx, writable);
                rowIdx += writable;
            } else {
                int di = 0;
                int writable = Math.min(pageNumValues, numRows - rowIdx);
                for (int i = 0; i < writable; i++) {
                    if (defLevels[i] == 1) out[rowIdx++] = decoded[di++];
                    else out[rowIdx++] = null;
                }
            }
        }
        return out;
    }

    private Object[] decodePlainValues(ByteBuffer buf, SchemaElement se, int n) {
        Type t = se.getType();
        Object[] out = new Object[n];
        boolean isDecimal = isLogicalDecimal(se);
        int decimalScale = isDecimal ? se.getScale() : 0;
        switch (t) {
            case BOOLEAN: {
                boolean[] bs = ParquetIo.decodePlainBooleans(buf, n);
                for (int i = 0; i < n; i++) out[i] = bs[i];
                return out;
            }
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
            case FLOAT: {
                ByteBuffer le = buf.slice().order(ByteOrder.LITTLE_ENDIAN);
                for (int i = 0; i < n; i++) out[i] = le.getFloat();
                buf.position(buf.position() + n * 4);
                return out;
            }
            case DOUBLE: {
                ByteBuffer le = buf.slice().order(ByteOrder.LITTLE_ENDIAN);
                for (int i = 0; i < n; i++) out[i] = le.getDouble();
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
                throw new UnsupportedOperationException("Physical type not supported: " + t);
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

    private Object convertToTarget(Object raw, Class<?> target) {
        if (raw == null) return null;
        if (target.isInstance(raw)) return raw;

        // Boxed/primitive numeric and boolean: target.isInstance handles boxed; primitive types
        // are auto-boxed by reflection. So Integer/Long/etc. are already covered above.

        if (target == BigDecimal.class) {
            if (raw instanceof BigDecimal bd) return bd;
            if (raw instanceof String s) return new BigDecimal(s);
        }
        if (target == LocalDate.class) {
            if (raw instanceof LocalDate d) return d;
            if (raw instanceof Integer i) return LocalDate.ofEpochDay(i);
            if (raw instanceof String s) return LocalDate.parse(s);
        }
        if (target == String.class) {
            return raw.toString();
        }
        // Numeric widening fallbacks
        if (raw instanceof Number n) {
            if (target == Integer.class || target == int.class) return n.intValue();
            if (target == Long.class || target == long.class) return n.longValue();
            if (target == Float.class || target == float.class) return n.floatValue();
            if (target == Double.class || target == double.class) return n.doubleValue();
        }
        throw new IllegalStateException("Cannot convert " + raw.getClass().getName()
                + " to " + target.getName());
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
