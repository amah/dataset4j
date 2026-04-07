package dataset4j.parquet;

import dataset4j.Dataset;
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
                Map<String, Object[]> columnData = new HashMap<>();
                for (ColumnChunk cc : rg.getColumns()) {
                    ColumnMetaData cmd = cc.getMeta_data();
                    String colName = cmd.getPath_in_schema().get(0);
                    SchemaElement se = schemaByName.get(colName);
                    Object[] values = readColumnChunk(ch, cmd, se, numRows);
                    columnData.put(colName, values);
                }

                for (int row = 0; row < numRows; row++) {
                    Object[] args = new Object[components.length];
                    for (int i = 0; i < components.length; i++) {
                        String name = components[i].getName();
                        Object[] colVals = columnData.get(name);
                        Object raw = colVals == null ? null : colVals[row];
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
        long offset = cmd.getData_page_offset();
        int totalCompressed = (int) cmd.getTotal_compressed_size();

        ByteBuffer chunkBuf = ByteBuffer.allocate(totalCompressed);
        ch.position(offset);
        readFully(ch, chunkBuf);
        byte[] chunkBytes = chunkBuf.array();

        ByteArrayInputStream bais = new ByteArrayInputStream(chunkBytes);
        int avail0 = bais.available();
        PageHeader ph = Util.readPageHeader(bais);
        int headerLen = avail0 - bais.available();

        if (ph.getType() != PageType.DATA_PAGE) {
            throw new IOException("Unsupported page type: " + ph.getType()
                    + " (only DATA_PAGE / DATA_PAGE_V1 is supported)");
        }
        DataPageHeader dph = ph.getData_page_header();
        if (dph.getEncoding() != Encoding.PLAIN) {
            throw new IOException("Unsupported value encoding: " + dph.getEncoding()
                    + " (only PLAIN is supported)");
        }

        int compressedPageSize = ph.getCompressed_page_size();
        int uncompressedPageSize = ph.getUncompressed_page_size();
        byte[] compressed = new byte[compressedPageSize];
        System.arraycopy(chunkBytes, headerLen, compressed, 0, compressedPageSize);

        ParquetCompressionCodec codec = fromThriftCodec(cmd.getCodec());
        byte[] uncompressed = ParquetIo.decompress(compressed, codec, uncompressedPageSize);
        ByteBuffer pageBuf = ByteBuffer.wrap(uncompressed);

        boolean optional = se.getRepetition_type() == FieldRepetitionType.OPTIONAL;
        int pageNumValues = dph.getNum_values();

        int[] defLevels;
        int nonNullCount;
        if (optional) {
            defLevels = ParquetIo.decodeDefLevels(pageBuf, pageNumValues);
            int nn = 0;
            for (int v : defLevels) if (v == 1) nn++;
            nonNullCount = nn;
        } else {
            defLevels = null;
            nonNullCount = pageNumValues;
        }

        Object[] decoded = decodePlainValues(pageBuf, se, nonNullCount);

        Object[] out = new Object[numRows];
        if (defLevels == null) {
            System.arraycopy(decoded, 0, out, 0, Math.min(decoded.length, numRows));
        } else {
            int di = 0;
            for (int i = 0; i < numRows; i++) {
                if (defLevels[i] == 1) out[i] = decoded[di++];
                else out[i] = null;
            }
        }
        return out;
    }

    private Object[] decodePlainValues(ByteBuffer buf, SchemaElement se, int n) {
        Type t = se.getType();
        Object[] out = new Object[n];
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
                    out[i] = isDate ? LocalDate.ofEpochDay(v) : Integer.valueOf(v);
                }
                buf.position(buf.position() + n * 4);
                return out;
            }
            case INT64: {
                ByteBuffer le = buf.slice().order(ByteOrder.LITTLE_ENDIAN);
                for (int i = 0; i < n; i++) out[i] = le.getLong();
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
                boolean isDecimal = isLogicalDecimal(se);
                int decimalScale = isDecimal ? se.getScale() : 0;
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
                        // Default: treat as UTF-8 string for unannotated BYTE_ARRAY
                        out[i] = new String(bytes, StandardCharsets.UTF_8);
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
