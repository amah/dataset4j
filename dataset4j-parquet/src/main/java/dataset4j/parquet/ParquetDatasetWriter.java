package dataset4j.parquet;

import dataset4j.Dataset;
import dataset4j.annotations.*;
import org.apache.parquet.format.*;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.RecordComponent;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Lightweight Parquet writer producing spec-compliant Parquet 1.0 files.
 *
 * <p>Writes a single data page per column chunk using PLAIN value encoding and
 * RLE/Bit-Packed Hybrid definition-level encoding (bit-width 1) for nullable columns.
 * Files are interoperable with standard Parquet readers (parquet-mr, pyarrow, DuckDB,
 * etc.) for the supported types.
 *
 * <p>Supported Java types: {@code Boolean, Integer, Long, Float, Double, String,
 * BigDecimal, LocalDate}. {@code BigDecimal} is stored as a UTF-8 string by default
 * (logical type STRING). Call {@link #withBigDecimalAsLogicalType(boolean)} to instead
 * use the proper {@code DECIMAL(precision, scale)} logical type — precision and scale
 * are derived from the data (max scale across all values; expanding scale never loses
 * precision).
 *
 * <p>Example:
 * <pre>{@code
 * ParquetDatasetWriter
 *     .toFile("employees.parquet")
 *     .withCompression(ParquetCompressionCodec.SNAPPY)
 *     .write(employees);
 * }</pre>
 */
public class ParquetDatasetWriter {

    private final Path filePath;
    private ParquetCompressionCodec compressionCodec = ParquetCompressionCodec.SNAPPY;
    private int rowGroupSize = 50000;
    private boolean bigDecimalAsLogicalDecimal = false;
    private final Map<String, String> keyValueMetadata = new HashMap<>();

    // Field selection support (preserved from previous API)
    private PojoMetadata<?> pojoMetadata;
    private FieldSelector<?> fieldSelector;

    private ParquetDatasetWriter(String filePath) {
        this.filePath = Paths.get(filePath);
    }

    /** Create a writer for the given output path. */
    public static ParquetDatasetWriter toFile(String filePath) {
        Path path = Paths.get(filePath).normalize();
        if (path.toString().contains("..")) {
            throw new SecurityException("Path traversal detected in file path: " + filePath);
        }
        return new ParquetDatasetWriter(filePath);
    }

    public ParquetDatasetWriter withCompression(ParquetCompressionCodec codec) {
        this.compressionCodec = codec;
        return this;
    }

    public ParquetDatasetWriter withRowGroupSize(int size) {
        this.rowGroupSize = size;
        return this;
    }

    /**
     * Encode {@code BigDecimal} columns using the Parquet {@code DECIMAL} logical type
     * (BYTE_ARRAY of two's-complement big-endian unscaled value) instead of the default
     * stringified representation. Precision/scale are derived from the data: scale is
     * the maximum scale found in non-null values across the dataset; precision is the
     * maximum digit count of the corresponding unscaled values.
     */
    public ParquetDatasetWriter withBigDecimalAsLogicalType(boolean enable) {
        this.bigDecimalAsLogicalDecimal = enable;
        return this;
    }

    /** Add a key/value entry to the file's key_value_metadata. */
    public ParquetDatasetWriter withMetadata(String key, String value) {
        this.keyValueMetadata.put(key, value);
        return this;
    }

    public ParquetDatasetWriter fields(String... fieldNames) {
        if (pojoMetadata != null) this.fieldSelector = FieldSelector.from(pojoMetadata).fields(fieldNames);
        return this;
    }

    public ParquetDatasetWriter columns(String... columnNames) {
        if (pojoMetadata != null) this.fieldSelector = FieldSelector.from(pojoMetadata).columns(columnNames);
        return this;
    }

    public ParquetDatasetWriter fieldsArray(String[] fieldConstants) {
        if (pojoMetadata != null) this.fieldSelector = FieldSelector.from(pojoMetadata).fieldsArray(fieldConstants);
        return this;
    }

    public ParquetDatasetWriter columnsArray(String[] columnConstants) {
        if (pojoMetadata != null) this.fieldSelector = FieldSelector.from(pojoMetadata).columnsArray(columnConstants);
        return this;
    }

    public ParquetDatasetWriter exclude(String... fieldNames) {
        if (pojoMetadata != null) {
            if (this.fieldSelector == null) this.fieldSelector = FieldSelector.from(pojoMetadata);
            this.fieldSelector = this.fieldSelector.exclude(fieldNames);
        }
        return this;
    }

    public ParquetDatasetWriter requiredOnly() {
        if (pojoMetadata != null) this.fieldSelector = FieldSelector.from(pojoMetadata).requiredOnly();
        return this;
    }

    public ParquetDatasetWriter exportableOnly() {
        if (pojoMetadata != null) this.fieldSelector = FieldSelector.from(pojoMetadata).exportableOnly();
        return this;
    }

    public <T> ParquetDatasetWriter select(PojoMetadata<T> metadata) {
        this.pojoMetadata = metadata;
        return this;
    }

    public <T> ParquetDatasetWriter select(FieldSelector<T> selector) {
        this.fieldSelector = selector;
        return this;
    }

    /** Write a Dataset to a Parquet file. */
    public <T> void write(Dataset<T> dataset) throws IOException {
        if (dataset.isEmpty()) {
            throw new IllegalArgumentException("Cannot write empty dataset");
        }

        List<T> records = dataset.toList();
        Class<?> recordClass = records.get(0).getClass();
        if (!recordClass.isRecord()) {
            throw new IllegalArgumentException("Dataset must contain record types");
        }

        if (pojoMetadata == null) {
            @SuppressWarnings("unchecked")
            Class<Object> typedClass = (Class<Object>) recordClass;
            pojoMetadata = MetadataCache.getMetadata(typedClass);
        }

        List<FieldMeta> fieldsToExport;
        if (fieldSelector != null) {
            fieldsToExport = fieldSelector.select();
        } else {
            fieldsToExport = pojoMetadata.getExportableFields();
        }
        if (fieldsToExport.isEmpty()) {
            throw new IllegalArgumentException("No fields selected for export. Ensure record has @DataColumn annotations.");
        }

        RecordComponent[] components = recordClass.getRecordComponents();
        List<LeafColumn> leaves = buildLeafColumns(fieldsToExport, components, records);

        try (FileOutputStream fos = new FileOutputStream(filePath.toFile());
             CountingOutputStream out = new CountingOutputStream(fos)) {

            // File magic
            out.write(ParquetIo.MAGIC);

            // Row groups
            List<RowGroup> rowGroups = new ArrayList<>();
            for (int start = 0; start < records.size(); start += rowGroupSize) {
                int end = Math.min(start + rowGroupSize, records.size());
                List<T> chunk = records.subList(start, end);
                rowGroups.add(writeRowGroup(out, chunk, leaves));
            }

            // Schema (root + leaves)
            List<SchemaElement> schema = buildSchemaElements(leaves);

            // FileMetaData
            FileMetaData fmd = new FileMetaData(1, schema, records.size(), rowGroups);
            fmd.setCreated_by("dataset4j-parquet");
            if (!keyValueMetadata.isEmpty()) {
                List<KeyValue> kvs = new ArrayList<>();
                for (Map.Entry<String, String> e : keyValueMetadata.entrySet()) {
                    KeyValue kv = new KeyValue(e.getKey());
                    kv.setValue(e.getValue());
                    kvs.add(kv);
                }
                fmd.setKey_value_metadata(kvs);
            }

            // Footer + length + magic
            long footerStart = out.getCount();
            Util.writeFileMetaData(fmd, out);
            int footerLength = (int) (out.getCount() - footerStart);
            out.write(intToLeBytes(footerLength));
            out.write(ParquetIo.MAGIC);
        }
    }

    // ---------- Row group / column chunk writing ----------

    private <T> RowGroup writeRowGroup(CountingOutputStream out, List<T> records, List<LeafColumn> leaves)
            throws IOException {
        long rgStart = out.getCount();
        List<ColumnChunk> chunks = new ArrayList<>();
        long totalUncompressed = 0;
        long totalCompressed = 0;

        for (LeafColumn leaf : leaves) {
            ColumnChunkResult ccr = writeColumnChunk(out, records, leaf);
            chunks.add(ccr.chunk);
            totalUncompressed += ccr.uncompressed;
            totalCompressed += ccr.compressed;
        }

        RowGroup rg = new RowGroup(chunks, totalUncompressed, records.size());
        rg.setFile_offset(rgStart);
        rg.setTotal_compressed_size(totalCompressed);
        return rg;
    }

    private <T> ColumnChunkResult writeColumnChunk(CountingOutputStream out, List<T> records, LeafColumn leaf)
            throws IOException {
        // Extract values
        List<Object> values = new ArrayList<>(records.size());
        int nonNull = 0;
        for (T r : records) {
            Object v;
            try {
                v = leaf.component.getAccessor().invoke(r);
            } catch (ReflectiveOperationException e) {
                throw new IOException("Failed to read field " + leaf.name, e);
            }
            values.add(v);
            if (v != null) nonNull++;
        }

        boolean optional = !leaf.required;

        // Build page payload: [def_levels?][plain_values]
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        if (optional) {
            int[] levels = new int[values.size()];
            for (int i = 0; i < values.size(); i++) levels[i] = values.get(i) == null ? 0 : 1;
            payload.write(ParquetIo.encodeDefLevels(levels));
        }
        byte[] plainValues = encodePlainValues(values, leaf, nonNull);
        payload.write(plainValues);

        byte[] uncompressed = payload.toByteArray();
        byte[] compressed = ParquetIo.compress(uncompressed, compressionCodec);

        // PageHeader (DATA_PAGE_V1)
        PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE, uncompressed.length, compressed.length);
        DataPageHeader dph = new DataPageHeader(values.size(),
                Encoding.PLAIN,
                Encoding.RLE,
                Encoding.RLE);
        pageHeader.setData_page_header(dph);

        long chunkStart = out.getCount();
        Util.writePageHeader(pageHeader, out);
        out.write(compressed);
        long chunkEnd = out.getCount();

        // ColumnMetaData
        ColumnMetaData cmd = new ColumnMetaData(
                leaf.physicalType,
                List.of(Encoding.RLE, Encoding.PLAIN),
                List.of(leaf.name),
                toThriftCodec(compressionCodec),
                values.size(),
                (long) (uncompressed.length + (chunkEnd - chunkStart - compressed.length)),
                chunkEnd - chunkStart,
                chunkStart);
        cmd.setData_page_offset(chunkStart);

        ColumnChunk chunk = new ColumnChunk(chunkStart);
        chunk.setMeta_data(cmd);

        ColumnChunkResult res = new ColumnChunkResult();
        res.chunk = chunk;
        res.uncompressed = cmd.getTotal_uncompressed_size();
        res.compressed = cmd.getTotal_compressed_size();
        return res;
    }

    private byte[] encodePlainValues(List<Object> values, LeafColumn leaf, int nonNull) throws IOException {
        Type t = leaf.physicalType;
        switch (t) {
            case BOOLEAN: {
                boolean[] bs = new boolean[nonNull];
                int i = 0;
                for (Object v : values) if (v != null) bs[i++] = (Boolean) v;
                return ParquetIo.encodePlainBooleans(bs);
            }
            case INT32: {
                ByteBuffer bb = ParquetIo.leBuffer(nonNull * 4);
                for (Object v : values) {
                    if (v == null) continue;
                    bb.putInt(toInt32(v, leaf));
                }
                return bb.array();
            }
            case INT64: {
                ByteBuffer bb = ParquetIo.leBuffer(nonNull * 8);
                for (Object v : values) {
                    if (v == null) continue;
                    bb.putLong((Long) v);
                }
                return bb.array();
            }
            case FLOAT: {
                ByteBuffer bb = ParquetIo.leBuffer(nonNull * 4);
                for (Object v : values) {
                    if (v == null) continue;
                    bb.putFloat((Float) v);
                }
                return bb.array();
            }
            case DOUBLE: {
                ByteBuffer bb = ParquetIo.leBuffer(nonNull * 8);
                for (Object v : values) {
                    if (v == null) continue;
                    bb.putDouble((Double) v);
                }
                return bb.array();
            }
            case BYTE_ARRAY: {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                for (Object v : values) {
                    if (v == null) continue;
                    byte[] bytes = byteArrayValue(v, leaf);
                    baos.write(intToLeBytes(bytes.length));
                    baos.write(bytes);
                }
                return baos.toByteArray();
            }
            default:
                throw new UnsupportedOperationException("Physical type not supported: " + t);
        }
    }

    private int toInt32(Object v, LeafColumn leaf) {
        if (v instanceof Integer i) return i;
        if (v instanceof LocalDate d) return (int) d.toEpochDay();
        throw new IllegalStateException("Cannot encode " + v.getClass() + " as INT32 for column " + leaf.name);
    }

    private byte[] byteArrayValue(Object v, LeafColumn leaf) {
        if (v instanceof String s) return s.getBytes(StandardCharsets.UTF_8);
        if (v instanceof BigDecimal bd) {
            if (leaf.decimalScale >= 0) {
                BigDecimal scaled = bd.setScale(leaf.decimalScale, java.math.RoundingMode.UNNECESSARY);
                BigInteger unscaled = scaled.unscaledValue();
                return unscaled.toByteArray(); // signed two's-complement BE
            }
            return bd.toPlainString().getBytes(StandardCharsets.UTF_8);
        }
        if (v instanceof byte[] b) return b;
        // Fallback: toString
        return v.toString().getBytes(StandardCharsets.UTF_8);
    }

    // ---------- Schema construction ----------

    private List<LeafColumn> buildLeafColumns(List<FieldMeta> fields, RecordComponent[] components, List<?> records) {
        List<LeafColumn> leaves = new ArrayList<>();
        for (FieldMeta fm : fields) {
            if (fm.isIgnored()) continue;
            RecordComponent comp = findComponent(components, fm.getFieldName());
            if (comp == null) continue;

            LeafColumn lc = new LeafColumn();
            lc.name = fm.getFieldName();
            lc.required = fm.isRequired();
            lc.component = comp;
            lc.javaType = comp.getType();

            Class<?> jt = lc.javaType;
            if (jt == Boolean.class || jt == boolean.class) {
                lc.physicalType = Type.BOOLEAN;
            } else if (jt == Integer.class || jt == int.class) {
                lc.physicalType = Type.INT32;
            } else if (jt == Long.class || jt == long.class) {
                lc.physicalType = Type.INT64;
            } else if (jt == Float.class || jt == float.class) {
                lc.physicalType = Type.FLOAT;
            } else if (jt == Double.class || jt == double.class) {
                lc.physicalType = Type.DOUBLE;
            } else if (jt == String.class) {
                lc.physicalType = Type.BYTE_ARRAY;
                lc.logicalType = LogicalType.STRING(new StringType());
                lc.convertedType = ConvertedType.UTF8;
            } else if (jt == LocalDate.class) {
                lc.physicalType = Type.INT32;
                lc.logicalType = LogicalType.DATE(new DateType());
                lc.convertedType = ConvertedType.DATE;
            } else if (jt == BigDecimal.class) {
                lc.physicalType = Type.BYTE_ARRAY;
                if (bigDecimalAsLogicalDecimal) {
                    int[] ps = computeDecimalPrecisionScale(records, comp);
                    lc.decimalPrecision = ps[0];
                    lc.decimalScale = ps[1];
                    lc.logicalType = LogicalType.DECIMAL(new DecimalType(ps[1], ps[0]));
                    lc.convertedType = ConvertedType.DECIMAL;
                } else {
                    lc.logicalType = LogicalType.STRING(new StringType());
                    lc.convertedType = ConvertedType.UTF8;
                }
            } else if (jt == byte[].class) {
                lc.physicalType = Type.BYTE_ARRAY;
            } else {
                throw new IllegalArgumentException("Unsupported field type for Parquet: " + jt.getName()
                        + " (field " + fm.getFieldName() + ")");
            }
            leaves.add(lc);
        }
        return leaves;
    }

    private static int[] computeDecimalPrecisionScale(List<?> records, RecordComponent comp) {
        int scale = 0;
        int precision = 1;
        for (Object r : records) {
            try {
                Object v = comp.getAccessor().invoke(r);
                if (v instanceof BigDecimal bd) {
                    if (bd.scale() > scale) scale = bd.scale();
                }
            } catch (ReflectiveOperationException ignored) {}
        }
        // Second pass with final scale to compute precision after rescaling
        for (Object r : records) {
            try {
                Object v = comp.getAccessor().invoke(r);
                if (v instanceof BigDecimal bd) {
                    BigDecimal rescaled = bd.setScale(scale, java.math.RoundingMode.UNNECESSARY);
                    int p = rescaled.unscaledValue().abs().toString().length();
                    if (p > precision) precision = p;
                }
            } catch (ReflectiveOperationException ignored) {}
        }
        return new int[]{precision, scale};
    }

    private List<SchemaElement> buildSchemaElements(List<LeafColumn> leaves) {
        List<SchemaElement> schema = new ArrayList<>();
        SchemaElement root = new SchemaElement("root");
        root.setNum_children(leaves.size());
        schema.add(root);

        for (LeafColumn lc : leaves) {
            SchemaElement se = new SchemaElement(lc.name);
            se.setType(lc.physicalType);
            se.setRepetition_type(lc.required ? FieldRepetitionType.REQUIRED : FieldRepetitionType.OPTIONAL);
            if (lc.logicalType != null) se.setLogicalType(lc.logicalType);
            if (lc.convertedType != null) se.setConverted_type(lc.convertedType);
            if (lc.decimalScale >= 0) {
                se.setScale(lc.decimalScale);
                se.setPrecision(lc.decimalPrecision);
            }
            schema.add(se);
        }
        return schema;
    }

    private static RecordComponent findComponent(RecordComponent[] components, String name) {
        for (RecordComponent c : components) if (c.getName().equals(name)) return c;
        return null;
    }

    private static CompressionCodec toThriftCodec(ParquetCompressionCodec c) {
        return switch (c) {
            case UNCOMPRESSED -> CompressionCodec.UNCOMPRESSED;
            case SNAPPY -> CompressionCodec.SNAPPY;
            case GZIP -> CompressionCodec.GZIP;
            case LZ4 -> CompressionCodec.LZ4_RAW;
            case BROTLI -> CompressionCodec.BROTLI;
        };
    }

    private static byte[] intToLeBytes(int v) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v).array();
    }

    // ---------- Helper types ----------

    private static final class LeafColumn {
        String name;
        boolean required;
        RecordComponent component;
        Class<?> javaType;
        Type physicalType;
        LogicalType logicalType;
        ConvertedType convertedType;
        int decimalPrecision = -1;
        int decimalScale = -1;
    }

    private static final class ColumnChunkResult {
        ColumnChunk chunk;
        long uncompressed;
        long compressed;
    }

    /** OutputStream that tracks the total number of bytes written. */
    private static final class CountingOutputStream extends FilterOutputStream {
        private long count = 0;
        CountingOutputStream(OutputStream out) { super(out); }
        @Override public void write(int b) throws IOException { out.write(b); count++; }
        @Override public void write(byte[] b, int off, int len) throws IOException { out.write(b, off, len); count += len; }
        long getCount() { return count; }
    }
}
