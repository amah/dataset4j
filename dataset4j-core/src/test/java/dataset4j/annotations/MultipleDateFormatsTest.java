package dataset4j.annotations;

import dataset4j.Dataset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MultipleDateFormatsTest {
    
    // Test record with multiple date format support
    record Transaction(
        @DataColumn(name = "Transaction Date", 
                   dateFormat = "yyyy-MM-dd",
                   alternativeDateFormats = {"MM/dd/yyyy", "dd-MMM-yyyy"})
        LocalDate date,
        
        @DataColumn(name = "Amount")
        double amount,
        
        @DataColumn(name = "Description")
        String description
    ) {}
    
    record Event(
        @DataColumn(name = "Event Time",
                   dateFormat = "yyyy-MM-dd HH:mm:ss",
                   alternativeDateFormats = {"MM/dd/yyyy HH:mm", "dd-MMM-yyyy HH:mm:ss"})
        LocalDateTime eventTime,
        
        @DataColumn(name = "Event Name")
        String name
    ) {}
    
    record LegacyRecord(
        @DataColumn(name = "Created Date",
                   dateFormat = "yyyy-MM-dd",
                   alternativeDateFormats = {"MM/dd/yyyy", "dd/MM/yyyy"})
        Date createdDate,
        
        @DataColumn(name = "ID")
        String id
    ) {}
    
    @Nested
    class LocalDateParsing {
        
        @Test
        void testPrimaryDateFormat() {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(Transaction.class);
            ColumnMetadata dateColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("date"))
                .findFirst().orElseThrow();
            
            // Test primary format (yyyy-MM-dd)
            Object result = FormatProvider.parseValue("2024-03-15", dateColumn);
            assertEquals(LocalDate.of(2024, 3, 15), result);
        }
        
        @Test
        void testAlternativeDateFormat1() {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(Transaction.class);
            ColumnMetadata dateColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("date"))
                .findFirst().orElseThrow();
            
            // Test first alternative format (MM/dd/yyyy)
            Object result = FormatProvider.parseValue("03/15/2024", dateColumn);
            assertEquals(LocalDate.of(2024, 3, 15), result);
        }
        
        @Test
        void testAlternativeDateFormat2() {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(Transaction.class);
            ColumnMetadata dateColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("date"))
                .findFirst().orElseThrow();
            
            // Test second alternative format (dd-MMM-yyyy)
            Object result = FormatProvider.parseValue("15-Mar-2024", dateColumn);
            assertEquals(LocalDate.of(2024, 3, 15), result);
        }
        
        @Test
        void testInvalidDateFormat() {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(Transaction.class);
            ColumnMetadata dateColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("date"))
                .findFirst().orElseThrow();
            
            // Test with format that doesn't match any configured format
            assertThrows(IllegalArgumentException.class, 
                () -> FormatProvider.parseValue("2024.03.15", dateColumn));
        }
    }
    
    @Nested
    class LocalDateTimeParsing {
        
        @Test
        void testPrimaryDateTimeFormat() {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(Event.class);
            ColumnMetadata timeColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("eventTime"))
                .findFirst().orElseThrow();
            
            // Test primary format
            Object result = FormatProvider.parseValue("2024-03-15 14:30:00", timeColumn);
            assertEquals(LocalDateTime.of(2024, 3, 15, 14, 30, 0), result);
        }
        
        @Test
        void testAlternativeDateTimeFormat() {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(Event.class);
            ColumnMetadata timeColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("eventTime"))
                .findFirst().orElseThrow();
            
            // Test alternative format
            Object result = FormatProvider.parseValue("03/15/2024 14:30", timeColumn);
            assertEquals(LocalDateTime.of(2024, 3, 15, 14, 30, 0), result);
        }
        
        @Test
        void testDateOnlyFormatForDateTime() {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(Event.class);
            ColumnMetadata timeColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("eventTime"))
                .findFirst().orElseThrow();
            
            // Test date-only format (should default to start of day)
            Object result = FormatProvider.parseValue("2024-03-15", timeColumn);
            assertEquals(LocalDateTime.of(2024, 3, 15, 0, 0, 0), result);
        }
    }
    
    @Nested
    class LegacyDateParsing {
        
        @Test
        void testPrimaryLegacyDateFormat() throws Exception {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(LegacyRecord.class);
            ColumnMetadata dateColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("createdDate"))
                .findFirst().orElseThrow();
            
            // Test primary format
            Object result = FormatProvider.parseValue("2024-03-15", dateColumn);
            
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date expected = sdf.parse("2024-03-15");
            assertEquals(expected, result);
        }
        
        @Test
        void testAlternativeLegacyDateFormat() throws Exception {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(LegacyRecord.class);
            ColumnMetadata dateColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("createdDate"))
                .findFirst().orElseThrow();
            
            // Test alternative US format
            Object result = FormatProvider.parseValue("03/15/2024", dateColumn);
            
            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
            Date expected = sdf.parse("03/15/2024");
            assertEquals(expected, result);
        }
    }
    
    @Nested
    class MetadataExtraction {
        
        @Test
        void testAlternativeDateFormatsExtracted() {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(Transaction.class);
            ColumnMetadata dateColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("date"))
                .findFirst().orElseThrow();
            
            assertEquals("yyyy-MM-dd", dateColumn.getDateFormat());
            assertArrayEquals(
                new String[]{"MM/dd/yyyy", "dd-MMM-yyyy"}, 
                dateColumn.getAlternativeDateFormats()
            );
        }
        
        @Test
        void testEmptyAlternativeDateFormats() {
            // Record without alternative formats
            record SimpleRecord(
                @DataColumn(name = "Date", dateFormat = "yyyy-MM-dd")
                LocalDate date
            ) {}
            
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(SimpleRecord.class);
            ColumnMetadata dateColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("date"))
                .findFirst().orElseThrow();
            
            assertEquals("yyyy-MM-dd", dateColumn.getDateFormat());
            assertArrayEquals(new String[]{}, dateColumn.getAlternativeDateFormats());
        }
    }
    
    @Nested
    class ErrorHandling {
        
        @Test
        void testDetailedErrorMessage() {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(Transaction.class);
            ColumnMetadata dateColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("date"))
                .findFirst().orElseThrow();
            
            // Test that error message includes all attempted formats
            IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> FormatProvider.parseValue("invalid-date", dateColumn)
            );
            
            String message = exception.getMessage();
            assertTrue(message.contains("yyyy-MM-dd"));
            assertTrue(message.contains("MM/dd/yyyy"));
            assertTrue(message.contains("dd-MMM-yyyy"));
        }
        
        @Test
        void testNullAndEmptyHandling() {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(Transaction.class);
            ColumnMetadata dateColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("date"))
                .findFirst().orElseThrow();
            
            // Null should return null (unless required)
            assertNull(FormatProvider.parseValue(null, dateColumn));
            assertNull(FormatProvider.parseValue("", dateColumn));
            assertNull(FormatProvider.parseValue("  ", dateColumn));
        }
    }
}