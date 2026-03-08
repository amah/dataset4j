package dataset4j.annotations;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Thread-safe cache for POJO metadata to avoid repeated reflection operations.
 * This cache significantly improves performance for repeated metadata access.
 * 
 * <p>Example usage:
 * {@code
 * // Get cached metadata (computed on first access)
 * PojoMetadata<Employee> metadata = MetadataCache.getMetadata(Employee.class);
 * 
 * // Clear cache if needed (e.g., during testing)
 * MetadataCache.clear();
 * 
 * // Check cache statistics
 * int cacheSize = MetadataCache.size();
 * }
 */
public final class MetadataCache {
    
    private static final ConcurrentMap<Class<?>, PojoMetadata<?>> cache = new ConcurrentHashMap<>();
    
    private MetadataCache() {
        // Utility class
    }
    
    /**
     * Get metadata for a record class, using cache if available.
     * This is the primary method for accessing metadata in production code.
     * 
     * @param <T> the record type
     * @param recordClass the record class
     * @return cached or newly computed metadata
     * @throws IllegalArgumentException if the class is not a record
     */
    @SuppressWarnings("unchecked")
    public static <T> PojoMetadata<T> getMetadata(Class<T> recordClass) {
        if (!recordClass.isRecord()) {
            throw new IllegalArgumentException("Class must be a record: " + recordClass.getName());
        }
        
        return (PojoMetadata<T>) cache.computeIfAbsent(recordClass, clazz -> {
            @SuppressWarnings("unchecked")
            Class<T> typedClass = (Class<T>) clazz;
            return PojoMetadata.of(typedClass);
        });
    }
    
    /**
     * Check if metadata for a class is cached.
     * 
     * @param recordClass the record class
     * @return true if metadata is cached, false otherwise
     */
    public static boolean isCached(Class<?> recordClass) {
        return cache.containsKey(recordClass);
    }
    
    /**
     * Put metadata into cache manually.
     * Useful for testing or pre-loading metadata.
     * 
     * @param <T> the record type
     * @param recordClass the record class
     * @param metadata the metadata to cache
     */
    public static <T> void putMetadata(Class<T> recordClass, PojoMetadata<T> metadata) {
        cache.put(recordClass, metadata);
    }
    
    /**
     * Remove metadata from cache.
     * 
     * @param recordClass the record class to remove
     * @return the removed metadata, or null if not present
     */
    @SuppressWarnings("unchecked")
    public static <T> PojoMetadata<T> removeMetadata(Class<T> recordClass) {
        return (PojoMetadata<T>) cache.remove(recordClass);
    }
    
    /**
     * Clear all cached metadata.
     * Useful for testing or memory management.
     */
    public static void clear() {
        cache.clear();
    }
    
    /**
     * Get the number of cached metadata entries.
     * 
     * @return cache size
     */
    public static int size() {
        return cache.size();
    }
    
    /**
     * Check if the cache is empty.
     * 
     * @return true if cache is empty, false otherwise
     */
    public static boolean isEmpty() {
        return cache.isEmpty();
    }
    
    /**
     * Get cache statistics for monitoring.
     * 
     * @return cache statistics
     */
    public static CacheStats getStats() {
        return new CacheStats(cache.size(), cache.keySet());
    }
    
    /**
     * Statistics about the metadata cache.
     */
    public static class CacheStats {
        private final int size;
        private final java.util.Set<Class<?>> cachedClasses;
        
        private CacheStats(int size, java.util.Set<Class<?>> cachedClasses) {
            this.size = size;
            this.cachedClasses = new java.util.HashSet<>(cachedClasses);
        }
        
        /**
         * Get the number of cached classes.
         * @return cache size
         */
        public int getSize() {
            return size;
        }
        
        /**
         * Get all cached class names.
         * @return set of class names
         */
        public java.util.Set<String> getCachedClassNames() {
            return cachedClasses.stream()
                    .map(Class::getName)
                    .collect(java.util.stream.Collectors.toSet());
        }
        
        /**
         * Check if a specific class is cached.
         * @param recordClass the class to check
         * @return true if cached, false otherwise
         */
        public boolean isCached(Class<?> recordClass) {
            return cachedClasses.contains(recordClass);
        }
        
        @Override
        public String toString() {
            return String.format("CacheStats[size=%d, classes=%s]",
                    size, getCachedClassNames());
        }
    }
}