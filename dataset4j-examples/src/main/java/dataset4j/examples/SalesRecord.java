package dataset4j.examples;

import dataset4j.annotations.*;
import jakarta.validation.constraints.*;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Example record demonstrating structured data annotations for sales reporting.
 * Shows advanced data formatting and styling options.
 */
@DataTable(
    name = "Sales Report",
    description = "Daily sales transaction data",
    headers = true,
    validateOnImport = true
)
public record SalesRecord(
    
    @DataColumn(name = "Transaction ID", order = 1, required = true,
                columnIndex = 0, cellType = DataColumn.CellType.TEXT, 
                frozen = true, width = 15, bold = true)
    @NotBlank
    @Pattern(regexp = "TXN-\\d{8}", message = "Transaction ID must follow format TXN-XXXXXXXX")
    String transactionId,
    
    @DataColumn(name = "Sale Date", order = 2, required = true,
                columnIndex = 1, cellType = DataColumn.CellType.DATE, 
                dateFormat = "MM/dd/yyyy", alignment = DataColumn.Alignment.CENTER, width = 12)
    @NotNull
    LocalDate saleDate,
    
    @DataColumn(name = "Customer ID", order = 3,
                columnIndex = 2, cellType = DataColumn.CellType.TEXT, width = 12)
    @Pattern(regexp = "CUST-\\d{4}|GUEST", message = "Customer ID must be CUST-XXXX or GUEST")
    String customerId,
    
    @DataColumn(name = "Product SKU", order = 4, required = true,
                columnIndex = 3, cellType = DataColumn.CellType.TEXT, width = 15)
    @NotBlank
    @Pattern(regexp = "SKU-[A-Z0-9]{6}", message = "Product SKU must follow format SKU-XXXXXX")
    String productSku,
    
    @DataColumn(name = "Quantity", order = 5, required = true,
                columnIndex = 4, cellType = DataColumn.CellType.NUMBER, 
                numberFormat = "0", alignment = DataColumn.Alignment.RIGHT, width = 10)
    @NotNull
    @Min(value = 1, message = "Quantity must be at least 1")
    @Max(value = 100, message = "Quantity cannot exceed 100")
    Integer quantity,
    
    @DataColumn(name = "Unit Price", order = 6, required = true,
                columnIndex = 5, cellType = DataColumn.CellType.CURRENCY, 
                numberFormat = "$#,##0.00", alignment = DataColumn.Alignment.RIGHT, width = 12)
    @NotNull
    @DecimalMin(value = "0.01", message = "Unit price must be at least $0.01")
    @DecimalMax(value = "10000.00", message = "Unit price cannot exceed $10,000")
    BigDecimal unitPrice,
    
    @DataColumn(name = "Total Amount", order = 7, required = true,
                columnIndex = 6, cellType = DataColumn.CellType.CURRENCY, 
                numberFormat = "$#,##0.00", alignment = DataColumn.Alignment.RIGHT, width = 12,
                backgroundColor = "#E6F3FF", bold = true)
    @NotNull
    @DecimalMin(value = "0.01", message = "Total amount must be positive")
    BigDecimal totalAmount,
    
    @DataColumn(name = "Discount %", order = 8,
                columnIndex = 7, cellType = DataColumn.CellType.PERCENTAGE, 
                numberFormat = "0.0%", alignment = DataColumn.Alignment.RIGHT, width = 10)
    @DecimalMin(value = "0", message = "Discount cannot be negative")
    @DecimalMax(value = "50", message = "Discount cannot exceed 50%")
    Double discountPercentage,
    
    @DataColumn(name = "Tax Amount", order = 9,
                columnIndex = 8, cellType = DataColumn.CellType.CURRENCY, 
                numberFormat = "$#,##0.00", alignment = DataColumn.Alignment.RIGHT, width = 12)
    @DecimalMin(value = "0", message = "Tax amount cannot be negative")
    BigDecimal taxAmount,
    
    @DataColumn(name = "Sales Rep", order = 10,
                columnIndex = 9, cellType = DataColumn.CellType.TEXT, width = 20)
    @Pattern(regexp = "EMP-\\d{4}", message = "Sales rep must be valid employee ID")
    String salesRepId,
    
    @DataColumn(name = "Payment Method", order = 11,
                columnIndex = 10, cellType = DataColumn.CellType.TEXT, 
                alignment = DataColumn.Alignment.CENTER, width = 15)
    @Pattern(regexp = "Cash|Credit Card|Debit Card|Check|Gift Card|Store Credit",
             message = "Payment method must be one of: Cash, Credit Card, Debit Card, Check, Gift Card, Store Credit")
    String paymentMethod,
    
    @DataColumn(name = "Status", order = 12, defaultValue = "Completed",
                columnIndex = 11, cellType = DataColumn.CellType.TEXT, 
                alignment = DataColumn.Alignment.CENTER, width = 12, bold = true)
    @Pattern(regexp = "Pending|Completed|Refunded|Cancelled",
             message = "Status must be one of: Pending, Completed, Refunded, Cancelled")
    String status,
    
    @DataColumn(name = "Notes", order = 13,
                columnIndex = 12, cellType = DataColumn.CellType.TEXT, 
                width = 30, wrapText = true)
    @Size(max = 500, message = "Notes cannot exceed 500 characters")
    String notes
) {
    
    /**
     * Calculate the net amount after discount and tax.
     */
    public BigDecimal getNetAmount() {
        BigDecimal discountAmount = BigDecimal.ZERO;
        if (discountPercentage != null && discountPercentage > 0) {
            discountAmount = totalAmount.multiply(BigDecimal.valueOf(discountPercentage / 100));
        }
        
        BigDecimal netBeforeTax = totalAmount.subtract(discountAmount);
        BigDecimal tax = taxAmount != null ? taxAmount : BigDecimal.ZERO;
        
        return netBeforeTax.add(tax);
    }
    
    /**
     * Check if this sale qualifies for commission.
     */
    public boolean isCommissionEligible() {
        return "Completed".equals(status) && 
               totalAmount.compareTo(new BigDecimal("50.00")) >= 0;
    }
    
    /**
     * Get the effective discount amount in dollars.
     */
    public BigDecimal getDiscountAmount() {
        if (discountPercentage == null || discountPercentage == 0) {
            return BigDecimal.ZERO;
        }
        
        return totalAmount.multiply(BigDecimal.valueOf(discountPercentage / 100));
    }
    
    /**
     * Check if this is a high-value transaction.
     */
    public boolean isHighValue() {
        return totalAmount.compareTo(new BigDecimal("1000.00")) >= 0;
    }
    
    /**
     * Get a summary description of the sale.
     */
    public String getSaleSummary() {
        return String.format("%d × %s = %s (%s)", 
            quantity, productSku, totalAmount, status);
    }
}