package dataset4j.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Per-read value pool that returns a single canonical instance for equal values.
 *
 * <p>Intended scope: one instance per read invocation, then discarded. Not thread-safe
 * and not meant to live across reads — that would unbound memory growth.
 *
 * <p>Useful for collapsing repeated immutable values (e.g. the same {@code String},
 * {@code Double}, {@code BigDecimal}, {@code LocalDate}, or {@link dataset4j.CellValue})
 * that appear many times in a dataset so that only one heap object is retained.
 *
 * <p>Has no effect on values that the JVM already canonicalizes ({@link Boolean#TRUE},
 * {@link Boolean#FALSE}, cached small {@code Integer}/{@code Long} values) — those flow
 * through harmlessly.
 */
public final class ValuePool {

    private final Map<Object, Object> pool = new HashMap<>();

    /**
     * Return the canonical instance for {@code value}: if an {@code equals}-equal
     * instance has already been seen, return that; otherwise remember and return
     * the given value.
     *
     * @param value the value to canonicalize (may be {@code null})
     * @param <T> the value type
     * @return the canonical instance, or {@code null} if {@code value} is {@code null}
     */
    @SuppressWarnings("unchecked")
    public <T> T intern(T value) {
        if (value == null) {
            return null;
        }
        Object existing = pool.putIfAbsent(value, value);
        return existing != null ? (T) existing : value;
    }

    /** @return number of unique values currently pooled (for diagnostics/tests) */
    public int size() {
        return pool.size();
    }
}
