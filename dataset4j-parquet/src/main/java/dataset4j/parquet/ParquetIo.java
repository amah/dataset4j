package dataset4j.parquet;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Internal helpers for spec-compliant Parquet I/O: PLAIN value encoding/decoding,
 * RLE/Bit-Packed Hybrid level encoding/decoding (for bit-width 1), and codec-based
 * compression/decompression. Package-private; not part of the public API.
 */
final class ParquetIo {

    static final byte[] MAGIC = {'P', 'A', 'R', '1'};

    private ParquetIo() {}

    // ---------- Compression ----------

    static byte[] compress(byte[] data, ParquetCompressionCodec codec) throws IOException {
        return switch (codec) {
            case UNCOMPRESSED -> data;
            case SNAPPY -> org.xerial.snappy.Snappy.compress(data);
            case GZIP -> {
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                     GZIPOutputStream gz = new GZIPOutputStream(baos)) {
                    gz.write(data);
                    gz.finish();
                    yield baos.toByteArray();
                }
            }
            case LZ4 -> {
                net.jpountz.lz4.LZ4Factory factory = net.jpountz.lz4.LZ4Factory.fastestInstance();
                net.jpountz.lz4.LZ4Compressor compressor = factory.fastCompressor();
                int max = compressor.maxCompressedLength(data.length);
                byte[] buf = new byte[max];
                int n = compressor.compress(data, 0, data.length, buf, 0, max);
                byte[] out = new byte[n];
                System.arraycopy(buf, 0, out, 0, n);
                yield out;
            }
            default -> throw new UnsupportedOperationException("Compression codec not supported: " + codec);
        };
    }

    static byte[] decompress(byte[] data, ParquetCompressionCodec codec, int uncompressedSize) throws IOException {
        return switch (codec) {
            case UNCOMPRESSED -> data;
            case SNAPPY -> org.xerial.snappy.Snappy.uncompress(data);
            case GZIP -> {
                try (GZIPInputStream gz = new GZIPInputStream(new ByteArrayInputStream(data));
                     ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                    byte[] buf = new byte[4096];
                    int r;
                    while ((r = gz.read(buf)) != -1) baos.write(buf, 0, r);
                    yield baos.toByteArray();
                }
            }
            case LZ4 -> {
                net.jpountz.lz4.LZ4Factory factory = net.jpountz.lz4.LZ4Factory.fastestInstance();
                net.jpountz.lz4.LZ4FastDecompressor decompressor = factory.fastDecompressor();
                yield decompressor.decompress(data, uncompressedSize);
            }
            default -> throw new UnsupportedOperationException("Compression codec not supported: " + codec);
        };
    }

    // ---------- Definition-level codec (RLE/Bit-Packed Hybrid, bit-width 1) ----------

    /**
     * Encode N definition-level values (0 or 1) using a single bit-packed run.
     * Output layout: 4-byte LE length prefix + RLE/bit-packed-hybrid stream.
     */
    static byte[] encodeDefLevels(int[] levels) {
        int n = levels.length;
        int numGroups = (n + 7) / 8;
        // varint header: (numGroups << 1) | 1
        long header = ((long) numGroups << 1) | 1L;
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        writeVarint(stream, header);
        // Each group: 8 levels bit-packed LSB-first into one byte (bit-width 1)
        for (int g = 0; g < numGroups; g++) {
            int b = 0;
            for (int i = 0; i < 8; i++) {
                int idx = g * 8 + i;
                int v = idx < n ? (levels[idx] & 1) : 0;
                b |= (v << i);
            }
            stream.write(b);
        }
        byte[] body = stream.toByteArray();
        ByteBuffer out = ByteBuffer.allocate(4 + body.length).order(ByteOrder.LITTLE_ENDIAN);
        out.putInt(body.length);
        out.put(body);
        return out.array();
    }

    /**
     * Decode definition levels from a DATA_PAGE_V1 levels block (length-prefixed RLE/bit-packed
     * hybrid). Advances the buffer past the levels block.
     *
     * @param buf       buffer positioned at the 4-byte LE length prefix
     * @param bitWidth  level bit-width (1 for max_def_level=1)
     * @param numValues total number of levels expected (page num_values)
     */
    static int[] decodeDefLevels(ByteBuffer buf, int bitWidth, int numValues) {
        ByteOrder prevOrder = buf.order();
        buf.order(ByteOrder.LITTLE_ENDIAN);
        int blockLen = buf.getInt();
        buf.order(prevOrder);
        int endPos = buf.position() + blockLen;
        int[] out = decodeHybrid(buf, bitWidth, numValues, endPos);
        if (buf.position() < endPos) buf.position(endPos);
        return out;
    }

    /**
     * Decode {@code numValues} integers from an RLE/Bit-Packed Hybrid stream of arbitrary
     * bit-width (1..32). Used both for definition levels (after length prefix) and for
     * dictionary indices in {@code RLE_DICTIONARY} pages (no length prefix; pass {@code endPos}
     * = {@code buf.limit()} or any safe upper bound).
     *
     * <p>The stream is a sequence of runs. Each run begins with an unsigned LEB128 varint header:
     * <ul>
     *   <li>{@code (header & 1) == 0} → RLE run: header {@code >>> 1} repetitions of a value
     *       encoded in {@code ceil(bitWidth/8)} little-endian bytes.</li>
     *   <li>{@code (header & 1) == 1} → bit-packed run: header {@code >>> 1} groups of 8 values,
     *       bit-packed LSB-first across {@code groups * bitWidth} bytes.</li>
     * </ul>
     */
    static int[] decodeHybrid(ByteBuffer buf, int bitWidth, int numValues, int endPos) {
        int[] out = new int[numValues];
        int filled = 0;
        if (bitWidth == 0) {
            // All values are zero (occurs when dictionary has a single entry).
            return out;
        }
        while (filled < numValues && buf.position() < endPos) {
            long header = readVarint(buf);
            boolean bitPacked = (header & 1L) == 1L;
            int runLen = (int) (header >>> 1);
            if (bitPacked) {
                // bit-packed run: runLen groups of 8 values, packed LSB-first
                int groupValues = runLen * 8;
                int totalBytes = runLen * bitWidth;
                long bitBuf = 0;
                int bitsInBuf = 0;
                int byteIdx = 0;
                int produced = 0;
                int mask = bitWidth == 32 ? -1 : ((1 << bitWidth) - 1);
                while (produced < groupValues) {
                    while (bitsInBuf < bitWidth && byteIdx < totalBytes) {
                        bitBuf |= ((long) (buf.get() & 0xFF)) << bitsInBuf;
                        bitsInBuf += 8;
                        byteIdx++;
                    }
                    if (bitsInBuf < bitWidth) break;
                    int v = (int) (bitBuf & 0xFFFFFFFFL & mask);
                    bitBuf >>>= bitWidth;
                    bitsInBuf -= bitWidth;
                    if (filled < numValues) out[filled++] = v;
                    produced++;
                }
            } else {
                // RLE run: a single value repeated runLen times
                int numBytes = (bitWidth + 7) / 8;
                int v = 0;
                for (int i = 0; i < numBytes; i++) {
                    v |= (buf.get() & 0xFF) << (i * 8);
                }
                int writable = Math.min(runLen, numValues - filled);
                for (int i = 0; i < writable; i++) out[filled++] = v;
            }
        }
        return out;
    }

    private static void writeVarint(ByteArrayOutputStream out, long value) {
        while ((value & ~0x7FL) != 0) {
            out.write((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.write((int) (value & 0x7F));
    }

    private static long readVarint(ByteBuffer buf) {
        long result = 0;
        int shift = 0;
        while (true) {
            int b = buf.get() & 0xFF;
            result |= ((long) (b & 0x7F)) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
            if (shift > 63) throw new IllegalStateException("Varint too long");
        }
    }

    // ---------- PLAIN encoding helpers ----------

    /** Allocate a little-endian buffer (Parquet PLAIN uses little-endian for fixed-width numerics). */
    static ByteBuffer leBuffer(int size) {
        return ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Encode N booleans as PLAIN bit-packed LSB-first (8 per byte).
     */
    static byte[] encodePlainBooleans(boolean[] values) {
        int n = values.length;
        int numBytes = (n + 7) / 8;
        byte[] out = new byte[numBytes];
        for (int i = 0; i < n; i++) {
            if (values[i]) out[i >>> 3] |= (byte) (1 << (i & 7));
        }
        return out;
    }

    /**
     * Decode N PLAIN bit-packed LSB-first booleans from buf, advancing position by ceil(n/8).
     */
    static boolean[] decodePlainBooleans(ByteBuffer buf, int n) {
        int numBytes = (n + 7) / 8;
        boolean[] out = new boolean[n];
        for (int i = 0; i < n; i++) {
            int bi = i >>> 3;
            int bit = i & 7;
            int b = buf.get(buf.position() + bi) & 0xFF;
            out[i] = ((b >>> bit) & 1) == 1;
        }
        buf.position(buf.position() + numBytes);
        return out;
    }
}
