package dataset4j.parquet;

/**
 * Parquet primitive data types.
 */
public enum ParquetDataType {
    /** Boolean values */
    BOOLEAN,
    
    /** 32-bit signed integer */
    INT32,
    
    /** 64-bit signed integer */
    INT64,
    
    /** 96-bit signed integer (deprecated) */
    INT96,
    
    /** IEEE 32-bit floating point */
    FLOAT,
    
    /** IEEE 64-bit floating point */
    DOUBLE,
    
    /** Byte array (variable length) */
    BYTE_ARRAY,
    
    /** Fixed length byte array */
    FIXED_LEN_BYTE_ARRAY;
    
    /**
     * Map Java class to Parquet data type.
     * @param javaClass Java class
     * @return corresponding Parquet data type
     */
    public static ParquetDataType fromJavaClass(Class<?> javaClass) {
        if (javaClass == boolean.class || javaClass == Boolean.class) {
            return BOOLEAN;
        }
        if (javaClass == int.class || javaClass == Integer.class) {
            return INT32;
        }
        if (javaClass == long.class || javaClass == Long.class) {
            return INT64;
        }
        if (javaClass == float.class || javaClass == Float.class) {
            return FLOAT;
        }
        if (javaClass == double.class || javaClass == Double.class) {
            return DOUBLE;
        }
        if (javaClass == String.class) {
            return BYTE_ARRAY;
        }
        if (javaClass == byte[].class) {
            return BYTE_ARRAY;
        }
        
        // Default to byte array for complex types
        return BYTE_ARRAY;
    }
    
    /**
     * Get Java class for this Parquet data type.
     * @return corresponding Java class
     */
    public Class<?> getJavaClass() {
        return switch (this) {
            case BOOLEAN -> Boolean.class;
            case INT32 -> Integer.class;
            case INT64 -> Long.class;
            case FLOAT -> Float.class;
            case DOUBLE -> Double.class;
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> String.class;
            case INT96 -> Long.class; // Treat as long for simplicity
        };
    }
}