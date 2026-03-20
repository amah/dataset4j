package dataset4j.annotations;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utility class for formatting field values based on annotation metadata.
 * 
 * <p>This class provides format conversion services for different data types
 * based on the formatting rules specified in column annotations.
 */
public final class FormatProvider {
    
    // Thread-safe caches for formatters to avoid recreation
    private static final ConcurrentHashMap<String, DecimalFormat> numberFormatCache = new ConcurrentHashMap<>();
    private static final ThreadLocal<Map<String, SimpleDateFormat>> dateFormatCache = 
        ThreadLocal.withInitial(HashMap::new);
    private static final ConcurrentHashMap<String, DateTimeFormatter> dateTimeFormatCache = new ConcurrentHashMap<>();
    
    private FormatProvider() {
        // Utility class
    }
    
    /**
     * Format a value according to column metadata rules.
     * 
     * @param value the value to format
     * @param metadata the column metadata containing format rules
     * @return formatted string representation
     */
    public static String formatValue(Object value, ColumnMetadata metadata) {
        if (value == null) {
            return "";
        }
        
        Class<?> valueType = value.getClass();
        
        // Handle numeric formatting
        if (isNumeric(valueType) && !metadata.getNumberFormat().isEmpty()) {
            return formatNumber(value, metadata.getNumberFormat());
        }
        
        // Handle date formatting
        if (isDate(valueType) && !metadata.getDateFormat().isEmpty()) {
            return formatDate(value, metadata.getDateFormat());
        }
        
        // Handle string truncation
        String stringValue = value.toString();
        if (metadata.getMaxLength() > 0 && stringValue.length() > metadata.getMaxLength()) {
            stringValue = stringValue.substring(0, metadata.getMaxLength());
        }
        
        return stringValue;
    }
    
    /**
     * Parse a string value according to column metadata rules.
     * 
     * @param stringValue the string to parse
     * @param metadata the column metadata containing parse rules
     * @return parsed object of the appropriate type
     * @throws IllegalArgumentException if parsing fails
     */
    public static Object parseValue(String stringValue, ColumnMetadata metadata) {
        if (stringValue == null || stringValue.trim().isEmpty()) {
            if (metadata.isRequired()) {
                throw new IllegalArgumentException("Required field cannot be empty: " + metadata.getFieldName());
            }
            return getDefaultValue(metadata);
        }
        
        Class<?> fieldType = metadata.getFieldType();
        String trimmed = stringValue.trim();
        
        try {
            // Handle primitive and wrapper types
            if (fieldType == String.class) {
                return trimmed;
            } else if (fieldType == int.class || fieldType == Integer.class) {
                return Integer.parseInt(trimmed);
            } else if (fieldType == long.class || fieldType == Long.class) {
                return Long.parseLong(trimmed);
            } else if (fieldType == double.class || fieldType == Double.class) {
                return Double.parseDouble(trimmed);
            } else if (fieldType == float.class || fieldType == Float.class) {
                return Float.parseFloat(trimmed);
            } else if (fieldType == boolean.class || fieldType == Boolean.class) {
                return Boolean.parseBoolean(trimmed);
            } else if (fieldType == LocalDate.class) {
                return parseLocalDate(trimmed, metadata);
            } else if (fieldType == LocalDateTime.class) {
                return parseLocalDateTime(trimmed, metadata);
            } else if (fieldType == ZonedDateTime.class) {
                return parseZonedDateTime(trimmed, metadata);
            } else if (fieldType == OffsetDateTime.class) {
                return parseOffsetDateTime(trimmed, metadata);
            } else if (fieldType == Date.class) {
                return parseDate(trimmed, metadata);
            } else if (fieldType == java.math.BigDecimal.class) {
                return new java.math.BigDecimal(trimmed);
            }
            
            // Fallback: try to create from string
            return trimmed;
            
        } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format("Failed to parse value '%s' for field '%s' of type %s: %s",
                    stringValue, metadata.getFieldName(), fieldType.getSimpleName(), e.getMessage()), e);
        }
    }
    
    /**
     * Format a numeric value according to the specified pattern.
     */
    public static String formatNumber(Object value, String pattern) {
        if (value == null) return "";
        
        DecimalFormat formatter = numberFormatCache.computeIfAbsent(pattern, DecimalFormat::new);
        
        if (value instanceof Number) {
            return formatter.format(((Number) value).doubleValue());
        }
        
        return value.toString();
    }
    
    /**
     * Format a date value according to the specified pattern.
     */
    public static String formatDate(Object value, String pattern) {
        if (value == null) return "";
        
        try {
            if (value instanceof LocalDate) {
                return ((LocalDate) value).format(getDateTimeFormatter(pattern));
            } else if (value instanceof LocalDateTime) {
                return ((LocalDateTime) value).format(getDateTimeFormatter(pattern));
            } else if (value instanceof ZonedDateTime) {
                return ((ZonedDateTime) value).format(getDateTimeFormatter(pattern));
            } else if (value instanceof OffsetDateTime) {
                return ((OffsetDateTime) value).format(getDateTimeFormatter(pattern));
            } else if (value instanceof Date) {
                return getDateFormat(pattern).format((Date) value);
            }
        } catch (Exception e) {
            // Fallback to string representation
        }
        
        return value.toString();
    }
    
    /**
     * Check if a class represents a numeric type.
     */
    public static boolean isNumeric(Class<?> type) {
        return type == int.class || type == Integer.class ||
               type == long.class || type == Long.class ||
               type == double.class || type == Double.class ||
               type == float.class || type == Float.class;
    }
    
    /**
     * Check if a class represents a date type.
     */
    public static boolean isDate(Class<?> type) {
        return type == LocalDate.class ||
               type == LocalDateTime.class ||
               type == ZonedDateTime.class ||
               type == OffsetDateTime.class ||
               type == Date.class;
    }
    
    /**
     * Get cached DecimalFormat for number formatting.
     */
    private static DecimalFormat getNumberFormat(String pattern) {
        return numberFormatCache.computeIfAbsent(pattern, DecimalFormat::new);
    }
    
    /**
     * Get cached SimpleDateFormat for date formatting.
     */
    private static SimpleDateFormat getDateFormat(String pattern) {
        return dateFormatCache.get().computeIfAbsent(pattern, SimpleDateFormat::new);
    }
    
    /**
     * Get cached DateTimeFormatter for modern date formatting.
     */
    private static DateTimeFormatter getDateTimeFormatter(String pattern) {
        return dateTimeFormatCache.computeIfAbsent(pattern, DateTimeFormatter::ofPattern);
    }
    
    /**
     * Parse a LocalDate with multiple format attempts.
     */
    private static LocalDate parseLocalDate(String value, ColumnMetadata metadata) {
        // Try primary format first
        try {
            return LocalDate.parse(value, getDateTimeFormatter(metadata.getDateFormat()));
        } catch (Exception ignored) {
            // Try alternative formats
        }
        
        // Try alternative formats
        for (String format : metadata.getAlternativeDateFormats()) {
            try {
                return LocalDate.parse(value, getDateTimeFormatter(format));
            } catch (Exception ignored) {
                // Continue to next format
            }
        }
        
        // If all formats failed, throw exception with details
        throw new IllegalArgumentException(
            String.format("Unable to parse date '%s' with format '%s' or alternatives %s",
                value, metadata.getDateFormat(), 
                java.util.Arrays.toString(metadata.getAlternativeDateFormats())));
    }
    
    /**
     * Parse a LocalDateTime with multiple format attempts.
     */
    private static LocalDateTime parseLocalDateTime(String value, ColumnMetadata metadata) {
        // Handle date-only format (yyyy-MM-dd) special case
        if (value.length() == 10) {
            try {
                LocalDate date = LocalDate.parse(value, getDateTimeFormatter("yyyy-MM-dd"));
                return date.atStartOfDay();
            } catch (Exception ignored) {
                // Not a simple date format, continue with normal parsing
            }
        }
        
        // Try primary format first
        try {
            return LocalDateTime.parse(value, getDateTimeFormatter(metadata.getDateFormat()));
        } catch (Exception ignored) {
            // Try alternative formats
        }
        
        // Try alternative formats
        for (String format : metadata.getAlternativeDateFormats()) {
            try {
                return LocalDateTime.parse(value, getDateTimeFormatter(format));
            } catch (Exception ignored) {
                // Continue to next format
            }
        }
        
        // Try ISO format as a last resort (e.g. values produced by LocalDateTime.toString())
        try {
            return LocalDateTime.parse(value);
        } catch (Exception ignored) {
            // Fall through to error
        }

        // If all formats failed, throw exception with details
        throw new IllegalArgumentException(
            String.format("Unable to parse datetime '%s' with format '%s' or alternatives %s",
                value, metadata.getDateFormat(),
                java.util.Arrays.toString(metadata.getAlternativeDateFormats())));
    }
    
    /**
     * Parse a ZonedDateTime with multiple format attempts.
     */
    private static ZonedDateTime parseZonedDateTime(String value, ColumnMetadata metadata) {
        // Try primary format first
        try {
            return ZonedDateTime.parse(value, getDateTimeFormatter(metadata.getDateFormat()));
        } catch (Exception ignored) {
            // Try alternative formats
        }
        
        // Try alternative formats
        for (String format : metadata.getAlternativeDateFormats()) {
            try {
                return ZonedDateTime.parse(value, getDateTimeFormatter(format));
            } catch (Exception ignored) {
                // Continue to next format
            }
        }
        
        // Try ISO-8601 formats as fallback
        try {
            return ZonedDateTime.parse(value); // Default ISO parser
        } catch (Exception ignored) {
            // Continue
        }
        
        // If all formats failed, throw exception with details
        throw new IllegalArgumentException(
            String.format("Unable to parse ZonedDateTime '%s' with format '%s' or alternatives %s",
                value, metadata.getDateFormat(), 
                java.util.Arrays.toString(metadata.getAlternativeDateFormats())));
    }
    
    /**
     * Parse an OffsetDateTime with multiple format attempts.
     */
    private static OffsetDateTime parseOffsetDateTime(String value, ColumnMetadata metadata) {
        // Try primary format first
        try {
            return OffsetDateTime.parse(value, getDateTimeFormatter(metadata.getDateFormat()));
        } catch (Exception ignored) {
            // Try alternative formats
        }
        
        // Try alternative formats
        for (String format : metadata.getAlternativeDateFormats()) {
            try {
                return OffsetDateTime.parse(value, getDateTimeFormatter(format));
            } catch (Exception ignored) {
                // Continue to next format
            }
        }
        
        // Try ISO-8601 formats as fallback
        try {
            return OffsetDateTime.parse(value); // Default ISO parser
        } catch (Exception ignored) {
            // Continue
        }
        
        // If all formats failed, throw exception with details
        throw new IllegalArgumentException(
            String.format("Unable to parse OffsetDateTime '%s' with format '%s' or alternatives %s",
                value, metadata.getDateFormat(), 
                java.util.Arrays.toString(metadata.getAlternativeDateFormats())));
    }
    
    /**
     * Parse a Date with multiple format attempts.
     */
    private static Date parseDate(String value, ColumnMetadata metadata) {
        // Try primary format first
        try {
            return getDateFormat(metadata.getDateFormat()).parse(value);
        } catch (Exception ignored) {
            // Try alternative formats
        }
        
        // Try alternative formats
        for (String format : metadata.getAlternativeDateFormats()) {
            try {
                return getDateFormat(format).parse(value);
            } catch (Exception ignored) {
                // Continue to next format
            }
        }
        
        // If all formats failed, throw exception with details
        throw new IllegalArgumentException(
            String.format("Unable to parse date '%s' with format '%s' or alternatives %s",
                value, metadata.getDateFormat(), 
                java.util.Arrays.toString(metadata.getAlternativeDateFormats())));
    }
    
    /**
     * Get default value for a field type.
     */
    private static Object getDefaultValue(ColumnMetadata metadata) {
        if (!metadata.getDefaultValue().isEmpty()) {
            return parseValue(metadata.getDefaultValue(), 
                ColumnMetadata.builder(metadata.getRecordComponent())
                    .required(false)  // Don't validate required for default values
                    .build());
        }
        
        Class<?> type = metadata.getFieldType();
        if (type == int.class) return 0;
        if (type == long.class) return 0L;
        if (type == double.class) return 0.0;
        if (type == float.class) return 0.0f;
        if (type == boolean.class) return false;
        
        return null; // For reference types
    }
}