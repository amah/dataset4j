package dataset4j.examples;

import dataset4j.annotations.*;
import jakarta.validation.constraints.*;

import java.time.LocalDate;

/**
 * Example record demonstrating comprehensive column mapping annotations
 * for an employee data structure suitable for Excel/CSV export.
 */
@DataTable(
    name = "Employee Report", 
    description = "Comprehensive employee data export",
    headers = true,
    validateOnImport = true
)
public record Employee(
    
    @DataColumn(name = "Employee ID", order = 1, required = true,
                columnIndex = 0, cellType = DataColumn.CellType.TEXT, frozen = true)
    @NotBlank
    @Pattern(regexp = "EMP-\\d{4}", message = "Employee ID must follow pattern EMP-XXXX")
    String employeeId,
    
    @DataColumn(name = "First Name", order = 2, required = true,
                columnIndex = 1, cellType = DataColumn.CellType.TEXT)
    @NotBlank
    @Size(min = 2, max = 50)
    String firstName,
    
    @DataColumn(name = "Last Name", order = 3, required = true,
                columnIndex = 2, cellType = DataColumn.CellType.TEXT)
    @NotBlank
    @Size(min = 2, max = 50)
    String lastName,
    
    @DataColumn(name = "Email", order = 4, required = true,
                columnIndex = 3, cellType = DataColumn.CellType.TEXT, width = 30)
    @NotBlank
    @Email(message = "Please enter a valid email address")
    String email,
    
    @DataColumn(name = "Department", order = 5,
                columnIndex = 4, cellType = DataColumn.CellType.TEXT)
    @Pattern(regexp = "IT|HR|Finance|Marketing|Operations|Sales", 
             message = "Department must be one of: IT, HR, Finance, Marketing, Operations, Sales")
    String department,
    
    @DataColumn(name = "Salary", order = 6,
                columnIndex = 5, cellType = DataColumn.CellType.CURRENCY, 
                numberFormat = "$#,##0.00", alignment = DataColumn.Alignment.RIGHT)
    @DecimalMin(value = "20000.0", message = "Salary must be at least $20,000")
    @DecimalMax(value = "500000.0", message = "Salary cannot exceed $500,000")
    Double salary,
    
    @DataColumn(name = "Hire Date", order = 7,
                columnIndex = 6, cellType = DataColumn.CellType.DATE, 
                dateFormat = "yyyy-MM-dd", alignment = DataColumn.Alignment.CENTER)
    @NotNull
    LocalDate hireDate,
    
    @DataColumn(name = "Active", order = 8, defaultValue = "true",
                columnIndex = 7, cellType = DataColumn.CellType.BOOLEAN, 
                alignment = DataColumn.Alignment.CENTER)
    Boolean isActive,
    
    @DataColumn(name = "Manager ID", order = 9,
                columnIndex = 8, cellType = DataColumn.CellType.TEXT)
    @Pattern(regexp = "EMP-\\d{4}|N/A", message = "Manager ID must be valid employee ID or N/A")
    String managerId,
    
    @DataColumn(name = "Years of Experience", order = 10,
                columnIndex = 9, cellType = DataColumn.CellType.NUMBER, 
                numberFormat = "0", alignment = DataColumn.Alignment.RIGHT)
    @Min(value = 0, message = "Years of experience cannot be negative")
    @Max(value = 50, message = "Years of experience cannot exceed 50")
    Integer yearsOfExperience,
    
    // Internal field - ignored in exports
    @DataColumn(ignore = true)
    String internalNotes
) {
    
    /**
     * Get full name for display purposes.
     */
    public String getFullName() {
        return firstName + " " + lastName;
    }
    
    /**
     * Check if employee is a manager (has direct reports).
     */
    public boolean isManager() {
        // This would typically check against a dataset of employees
        return managerId == null || managerId.equals("N/A");
    }
    
    /**
     * Get display-friendly department name.
     */
    public String getDepartmentDisplay() {
        return department != null ? department : "Unassigned";
    }
    
    /**
     * Calculate years since hire date.
     */
    public int getYearsSinceHire() {
        if (hireDate == null) return 0;
        return LocalDate.now().getYear() - hireDate.getYear();
    }
}