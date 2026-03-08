package dataset4j.parquet;

/**
 * Compression codecs supported by the lightweight Parquet implementation.
 */
public enum ParquetCompressionCodec {
    /** No compression */
    UNCOMPRESSED,
    
    /** Snappy compression (fast) */
    SNAPPY,
    
    /** GZIP compression (better compression ratio) */
    GZIP,
    
    /** LZ4 compression (very fast) */
    LZ4,
    
    /** Brotli compression (best compression ratio) */
    BROTLI
}