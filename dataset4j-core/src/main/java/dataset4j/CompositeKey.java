package dataset4j;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

/**
 * A composite key for multi-column joins, internally used by Dataset join operations.
 * 
 * <p>This class allows joining on multiple fields by combining them into a single
 * composite key that implements proper equals/hashCode semantics.
 *
 * <pre>
 * // Join employees and departments on both dept AND location
 * employees.innerJoinMulti(departments,
 *     e -> CompositeKey.of(e.dept(), e.location()),
 *     d -> CompositeKey.of(d.dept(), d.location()));
 * </pre>
 */
public final class CompositeKey {
    
    private final Object[] components;
    private final int hashCode;
    
    private CompositeKey(Object[] components) {
        this.components = components.clone(); // defensive copy
        this.hashCode = Arrays.hashCode(this.components);
    }
    
    /**
     * Create a composite key from 2 components.
     */
    public static CompositeKey of(Object key1, Object key2) {
        return new CompositeKey(new Object[]{key1, key2});
    }
    
    /**
     * Create a composite key from 3 components.
     */
    public static CompositeKey of(Object key1, Object key2, Object key3) {
        return new CompositeKey(new Object[]{key1, key2, key3});
    }
    
    /**
     * Create a composite key from 4 components.
     */
    public static CompositeKey of(Object key1, Object key2, Object key3, Object key4) {
        return new CompositeKey(new Object[]{key1, key2, key3, key4});
    }
    
    /**
     * Create a composite key from any number of components.
     */
    public static CompositeKey of(Object... components) {
        if (components.length == 0) {
            throw new IllegalArgumentException("CompositeKey must have at least one component");
        }
        return new CompositeKey(components);
    }

    // ---------------------------------------------------------------
    // Static factory methods for ergonomic static imports
    // ---------------------------------------------------------------

    /**
     * Create a composite key from 2 components (for static import).
     * 
     * <pre>
     * import static dataset4j.CompositeKey.key;
     * 
     * employees.innerJoinMulti(departments,
     *     e -> key(e.dept(), e.location()),
     *     d -> key(d.dept(), d.location()));
     * </pre>
     */
    public static CompositeKey key(Object key1, Object key2) {
        return of(key1, key2);
    }
    
    /**
     * Create a composite key from 3 components (for static import).
     */
    public static CompositeKey key(Object key1, Object key2, Object key3) {
        return of(key1, key2, key3);
    }
    
    /**
     * Create a composite key from 4 components (for static import).
     */
    public static CompositeKey key(Object key1, Object key2, Object key3, Object key4) {
        return of(key1, key2, key3, key4);
    }
    
    /**
     * Create a composite key from any number of components (for static import).
     */
    public static CompositeKey key(Object... components) {
        return of(components);
    }

    // ---------------------------------------------------------------
    // Fluent accessor-based factory methods
    // ---------------------------------------------------------------

    /**
     * Create a composite key factory from 2 property accessors (for fluent joins).
     * 
     * <pre>
     * import static dataset4j.CompositeKey.on;
     * 
     * employees.innerJoinOn(departments,
     *     on(Employee::dept, Employee::location),
     *     on(Department::dept, Department::location));
     * </pre>
     */
    public static <T> KeyFactory2<T> on(Function<T, ?> accessor1, Function<T, ?> accessor2) {
        return new KeyFactory2<>(accessor1, accessor2);
    }

    /**
     * Create a composite key factory from 3 property accessors (for fluent joins).
     */
    public static <T> KeyFactory3<T> on(Function<T, ?> accessor1, Function<T, ?> accessor2, Function<T, ?> accessor3) {
        return new KeyFactory3<>(accessor1, accessor2, accessor3);
    }

    /**
     * Create a composite key factory from 4 property accessors (for fluent joins).
     */
    public static <T> KeyFactory4<T> on(Function<T, ?> accessor1, Function<T, ?> accessor2, Function<T, ?> accessor3, Function<T, ?> accessor4) {
        return new KeyFactory4<>(accessor1, accessor2, accessor3, accessor4);
    }

    /**
     * Helper class for 2-property composite key creation.
     */
    public static class KeyFactory2<T> implements Function<T, CompositeKey> {
        private final Function<T, ?> accessor1;
        private final Function<T, ?> accessor2;

        KeyFactory2(Function<T, ?> accessor1, Function<T, ?> accessor2) {
            this.accessor1 = accessor1;
            this.accessor2 = accessor2;
        }

        @Override
        public CompositeKey apply(T obj) {
            return key(accessor1.apply(obj), accessor2.apply(obj));
        }
    }

    /**
     * Helper class for 3-property composite key creation.
     */
    public static class KeyFactory3<T> implements Function<T, CompositeKey> {
        private final Function<T, ?> accessor1;
        private final Function<T, ?> accessor2;
        private final Function<T, ?> accessor3;

        KeyFactory3(Function<T, ?> accessor1, Function<T, ?> accessor2, Function<T, ?> accessor3) {
            this.accessor1 = accessor1;
            this.accessor2 = accessor2;
            this.accessor3 = accessor3;
        }

        @Override
        public CompositeKey apply(T obj) {
            return key(accessor1.apply(obj), accessor2.apply(obj), accessor3.apply(obj));
        }
    }

    /**
     * Helper class for 4-property composite key creation.
     */
    public static class KeyFactory4<T> implements Function<T, CompositeKey> {
        private final Function<T, ?> accessor1;
        private final Function<T, ?> accessor2;
        private final Function<T, ?> accessor3;
        private final Function<T, ?> accessor4;

        KeyFactory4(Function<T, ?> accessor1, Function<T, ?> accessor2, Function<T, ?> accessor3, Function<T, ?> accessor4) {
            this.accessor1 = accessor1;
            this.accessor2 = accessor2;
            this.accessor3 = accessor3;
            this.accessor4 = accessor4;
        }

        @Override
        public CompositeKey apply(T obj) {
            return key(accessor1.apply(obj), accessor2.apply(obj), accessor3.apply(obj), accessor4.apply(obj));
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        CompositeKey that = (CompositeKey) obj;
        return Arrays.equals(components, that.components);
    }
    
    @Override
    public int hashCode() {
        return hashCode; // pre-computed for performance
    }
    
    @Override
    public String toString() {
        return "CompositeKey" + Arrays.toString(components);
    }
    
    /**
     * Get the number of key components.
     */
    public int size() {
        return components.length;
    }
    
    /**
     * Get a specific component by index (0-based).
     */
    public Object get(int index) {
        return components[index];
    }
}