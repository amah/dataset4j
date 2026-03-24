package dataset4j.poi;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

class JavaToExcelDateFormatTest {

    @ParameterizedTest
    @CsvSource({
        // Patterns that are identical in Java and Excel
        "yyyy-MM-dd,          yyyy-MM-dd",
        "yyyy-MM-dd HH:mm:ss, yyyy-MM-dd HH:mm:ss",
        "dd/MM/yyyy,          dd/MM/yyyy",
        "HH:mm,               HH:mm",
        "hh:mm:ss,            hh:mm:ss",
        "MM/dd/yyyy,          MM/dd/yyyy",

        // AM/PM marker: Java 'a' → Excel 'AM/PM'
        "hh:mm a,             hh:mm AM/PM",
        "hh:mm:ss a,          hh:mm:ss AM/PM",

        // Day of week: Java E → Excel DDD/DDDD
        "EEE dd MMM yyyy,     DDD dd MMM yyyy",
        "EEEE dd MMMM yyyy,   DDDD dd MMMM yyyy",
        "E,                    DDD",
        "EE,                   DDD",

        // Fraction of second: Java SSS → Excel .000
        "HH:mm:ss.SSS,        HH:mm:ss.000",
        "HH:mm:ss.SS,         HH:mm:ss.00",
        "HH:mm:ss.S,          HH:mm:ss.0",

        // Combined
        "EEEE yyyy-MM-dd hh:mm:ss.SSS a, DDDD yyyy-MM-dd hh:mm:ss.000 AM/PM",
    })
    void shouldConvertPattern(String javaPattern, String expectedExcel) {
        assertEquals(expectedExcel, JavaToExcelDateFormat.convert(javaPattern));
    }

    @Test
    void shouldPreserveQuotedLiterals() {
        assertEquals("yyyy'T'HH:mm:ss", JavaToExcelDateFormat.convert("yyyy'T'HH:mm:ss"));
    }

    @Test
    void shouldHandleNullAndEmpty() {
        assertNull(JavaToExcelDateFormat.convert(null));
        assertEquals("", JavaToExcelDateFormat.convert(""));
    }
}
