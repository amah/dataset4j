package dataset4j;

/**
 * Thrown when a {@link CellValue} cannot be coerced to the requested target type.
 *
 * <p>Provides the original value, its source {@link ValueType}, and the
 * target type that was requested, so error messages are immediately actionable.
 */
public class CellCoercionException extends RuntimeException {

    private final Object sourceValue;
    private final ValueType sourceType;
    private final String targetType;

    public CellCoercionException(Object sourceValue, ValueType sourceType, String targetType) {
        super(buildMessage(sourceValue, sourceType, targetType));
        this.sourceValue = sourceValue;
        this.sourceType = sourceType;
        this.targetType = targetType;
    }

    public CellCoercionException(Object sourceValue, ValueType sourceType, String targetType, Throwable cause) {
        super(buildMessage(sourceValue, sourceType, targetType), cause);
        this.sourceValue = sourceValue;
        this.sourceType = sourceType;
        this.targetType = targetType;
    }

    public Object getSourceValue() { return sourceValue; }
    public ValueType getSourceType() { return sourceType; }
    public String getTargetType() { return targetType; }

    private static String buildMessage(Object value, ValueType sourceType, String targetType) {
        return String.format("Cannot coerce %s value '%s' to %s",
                sourceType, value, targetType);
    }
}
