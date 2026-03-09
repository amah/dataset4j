package dataset4j.examples;

import dataset4j.annotations.*;
import jakarta.validation.constraints.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Example record showing CSV-focused annotations for product catalog export.
 * Demonstrates simpler annotation usage with emphasis on CSV formatting.
 */
@DataTable(
    name = "Product Catalog",
    description = "Product inventory and pricing data",
    headers = true
)
public record Product(
    
    @DataColumn(name = "SKU", order = 1, required = true)
    @NotBlank
    @Pattern(regexp = "SKU-[A-Z0-9]{6}", message = "SKU must follow format SKU-XXXXXX")
    String sku,
    
    @DataColumn(name = "Product Name", order = 2, required = true, width = 100)
    @NotBlank
    @Size(min = 3, max = 100)
    String name,
    
    @DataColumn(name = "Category", order = 3)
    @Pattern(regexp = "Electronics|Clothing|Books|Home|Sports|Beauty", 
             message = "Category must be one of: Electronics, Clothing, Books, Home, Sports, Beauty")
    String category,
    
    @DataColumn(name = "Price", order = 4, required = true)
    @NotNull
    @DecimalMin(value = "0.01", message = "Price must be at least $0.01")
    @DecimalMax(value = "9999.99", message = "Price cannot exceed $9,999.99")
    BigDecimal price,
    
    @DataColumn(name = "Discounted Price", order = 5)
    @DecimalMin(value = "0.00", message = "Discounted price cannot be negative")
    BigDecimal discountPrice,
    
    @DataColumn(name = "Stock Quantity", order = 6, defaultValue = "0")
    @Min(value = 0, message = "Stock quantity cannot be negative")
    @Max(value = 99999, message = "Stock quantity cannot exceed 99,999")
    Integer stockQuantity,
    
    @DataColumn(name = "Description", order = 7, width = 500)
    String description,
    
    @DataColumn(name = "Brand", order = 8)
    @Size(max = 50, message = "Brand name cannot exceed 50 characters")
    String brand,
    
    @DataColumn(name = "Weight (lbs)", order = 9)
    @DecimalMin(value = "0.01", message = "Weight must be at least 0.01 lbs")
    @DecimalMax(value = "1000.00", message = "Weight cannot exceed 1000 lbs")
    Double weight,
    
    @DataColumn(name = "Available", order = 10, defaultValue = "true")
    Boolean isAvailable,
    
    @DataColumn(name = "Last Updated", order = 11)
    LocalDateTime lastUpdated,
    
    @DataColumn(name = "Tags", order = 12, width = 200)
    @Pattern(regexp = "^[a-zA-Z0-9,\\s]*$", message = "Tags can only contain letters, numbers, commas, and spaces")
    @Size(max = 200, message = "Tags cannot exceed 200 characters")
    String tags
) {
    
    /**
     * Calculate discount percentage if both prices are available.
     */
    public Double getDiscountPercentage() {
        if (price == null || discountPrice == null || 
            price.compareTo(BigDecimal.ZERO) == 0) {
            return null;
        }
        
        BigDecimal discount = price.subtract(discountPrice);
        return discount.divide(price, 4, BigDecimal.ROUND_HALF_UP)
                      .multiply(new BigDecimal("100"))
                      .doubleValue();
    }
    
    /**
     * Check if product is on sale.
     */
    public boolean isOnSale() {
        return discountPrice != null && 
               price != null && 
               discountPrice.compareTo(price) < 0;
    }
    
    /**
     * Check if product is in stock.
     */
    public boolean isInStock() {
        return stockQuantity != null && stockQuantity > 0;
    }
    
    /**
     * Get effective selling price (discounted or regular).
     */
    public BigDecimal getEffectivePrice() {
        return isOnSale() ? discountPrice : price;
    }
    
    /**
     * Generate a short display name for the product.
     */
    public String getDisplayName() {
        if (name.length() <= 30) {
            return name;
        }
        return name.substring(0, 27) + "...";
    }
}