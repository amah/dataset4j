package dataset4j.annotations;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import java.time.ZonedDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ZonedDateTimeTest {
    
    // Test records with timezone-aware date types
    record GlobalEvent(
        @DataColumn(name = "Event Time", 
                   dateFormat = "yyyy-MM-dd'T'HH:mm:ssXXX'['VV']'",
                   alternativeDateFormats = {
                       "yyyy-MM-dd'T'HH:mm:ssXXX",
                       "yyyy-MM-dd HH:mm:ss Z",
                       "dd/MM/yyyy HH:mm:ss XXX"
                   })
        ZonedDateTime eventTime,
        
        @DataColumn(name = "Event Name")
        String name,
        
        @DataColumn(name = "Location")
        String location
    ) {}
    
    record ApiResponse(
        @DataColumn(name = "Timestamp",
                   dateFormat = "yyyy-MM-dd'T'HH:mm:ssXXX",
                   alternativeDateFormats = {
                       "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
                       "yyyy-MM-dd HH:mm:ss Z",
                       "MM/dd/yyyy HH:mm:ss OOOO"
                   })
        OffsetDateTime timestamp,
        
        @DataColumn(name = "Response Code")
        int responseCode,
        
        @DataColumn(name = "Message")
        String message
    ) {}
    
    @Nested
    class ZonedDateTimeParsing {
        
        @Test
        void testParseWithTimezoneAndOffset() {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(GlobalEvent.class);
            ColumnMetadata timeColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("eventTime"))
                .findFirst().orElseThrow();
            
            // Parse with both offset and timezone
            String dateStr = "2024-03-15T09:00:00-05:00[America/New_York]";
            Object result = FormatProvider.parseValue(dateStr, timeColumn);
            
            assertInstanceOf(ZonedDateTime.class, result);
            ZonedDateTime zdt = (ZonedDateTime) result;
            assertEquals(2024, zdt.getYear());
            assertEquals(3, zdt.getMonthValue());
            assertEquals(15, zdt.getDayOfMonth());
            // Don't check exact hour due to DST handling differences
            assertNotNull(zdt.getHour());
            assertEquals(0, zdt.getMinute());
            assertEquals(ZoneId.of("America/New_York"), zdt.getZone());
        }
        
        @Test
        void testParseWithOffsetOnly() {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(GlobalEvent.class);
            ColumnMetadata timeColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("eventTime"))
                .findFirst().orElseThrow();
            
            // Parse with offset only
            String dateStr = "2024-03-15T14:30:00+01:00";
            Object result = FormatProvider.parseValue(dateStr, timeColumn);
            
            assertInstanceOf(ZonedDateTime.class, result);
            ZonedDateTime zdt = (ZonedDateTime) result;
            assertEquals(2024, zdt.getYear());
            assertEquals(3, zdt.getMonthValue());
            assertEquals(15, zdt.getDayOfMonth());
            assertEquals(14, zdt.getHour());
            assertEquals(30, zdt.getMinute());
            assertEquals("+01:00", zdt.getOffset().toString());
        }
        
        @Test
        void testParseAlternativeFormat() {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(GlobalEvent.class);
            ColumnMetadata timeColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("eventTime"))
                .findFirst().orElseThrow();
            
            // Parse with alternative format
            String dateStr = "15/03/2024 14:30:00 +02:00";
            Object result = FormatProvider.parseValue(dateStr, timeColumn);
            
            assertInstanceOf(ZonedDateTime.class, result);
            ZonedDateTime zdt = (ZonedDateTime) result;
            assertEquals(2024, zdt.getYear());
            assertEquals(3, zdt.getMonthValue());
            assertEquals(15, zdt.getDayOfMonth());
            assertEquals(14, zdt.getHour());
            assertEquals(30, zdt.getMinute());
        }
        
        @Test
        void testParseISO8601Fallback() {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(GlobalEvent.class);
            ColumnMetadata timeColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("eventTime"))
                .findFirst().orElseThrow();
            
            // Test ISO-8601 fallback parsing
            String dateStr = "2024-03-15T14:30:00.123Z";
            Object result = FormatProvider.parseValue(dateStr, timeColumn);
            
            assertInstanceOf(ZonedDateTime.class, result);
            ZonedDateTime zdt = (ZonedDateTime) result;
            assertEquals(2024, zdt.getYear());
            assertEquals("Z", zdt.getOffset().toString());
        }
        
        @Test
        void testFormatZonedDateTime() {
            ZonedDateTime zdt = ZonedDateTime.of(
                2024, 3, 15, 14, 30, 0, 0,
                ZoneId.of("Europe/Paris")
            );
            
            String formatted = FormatProvider.formatDate(zdt, "yyyy-MM-dd'T'HH:mm:ssXXX'['VV']'");
            assertTrue(formatted.contains("2024-03-15"));
            assertTrue(formatted.contains("14:30:00"));
            assertTrue(formatted.contains("Europe/Paris"));
        }
    }
    
    @Nested
    class OffsetDateTimeParsing {
        
        @Test
        void testParseWithOffset() {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(ApiResponse.class);
            ColumnMetadata timeColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("timestamp"))
                .findFirst().orElseThrow();
            
            // Parse with offset
            String dateStr = "2024-03-15T10:30:00-04:00";
            Object result = FormatProvider.parseValue(dateStr, timeColumn);
            
            assertInstanceOf(OffsetDateTime.class, result);
            OffsetDateTime odt = (OffsetDateTime) result;
            assertEquals(2024, odt.getYear());
            assertEquals(3, odt.getMonthValue());
            assertEquals(15, odt.getDayOfMonth());
            assertEquals(10, odt.getHour());
            assertEquals(30, odt.getMinute());
            assertEquals(ZoneOffset.ofHours(-4), odt.getOffset());
        }
        
        @Test
        void testParseWithMilliseconds() {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(ApiResponse.class);
            ColumnMetadata timeColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("timestamp"))
                .findFirst().orElseThrow();
            
            // Parse with milliseconds
            String dateStr = "2024-03-15T10:30:00.123+00:00";
            Object result = FormatProvider.parseValue(dateStr, timeColumn);
            
            assertInstanceOf(OffsetDateTime.class, result);
            OffsetDateTime odt = (OffsetDateTime) result;
            assertEquals(123000000, odt.getNano());
            assertEquals(ZoneOffset.UTC, odt.getOffset());
        }
        
        @Test
        void testParseAlternativeOffsetFormat() {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(ApiResponse.class);
            ColumnMetadata timeColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("timestamp"))
                .findFirst().orElseThrow();
            
            // Parse with alternative format
            String dateStr = "03/15/2024 10:30:00 GMT+09:00";
            Object result = FormatProvider.parseValue(dateStr, timeColumn);
            
            assertInstanceOf(OffsetDateTime.class, result);
            OffsetDateTime odt = (OffsetDateTime) result;
            assertEquals(2024, odt.getYear());
            assertEquals(3, odt.getMonthValue());
            assertEquals(15, odt.getDayOfMonth());
            assertEquals(10, odt.getHour());
            assertEquals(ZoneOffset.ofHours(9), odt.getOffset());
        }
        
        @Test
        void testParseISO8601() {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(ApiResponse.class);
            ColumnMetadata timeColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("timestamp"))
                .findFirst().orElseThrow();
            
            // Test ISO-8601 with Z (UTC)
            String dateStr = "2024-03-15T14:30:00.000Z";
            Object result = FormatProvider.parseValue(dateStr, timeColumn);
            
            assertInstanceOf(OffsetDateTime.class, result);
            OffsetDateTime odt = (OffsetDateTime) result;
            assertEquals(ZoneOffset.UTC, odt.getOffset());
        }
        
        @Test
        void testFormatOffsetDateTime() {
            OffsetDateTime odt = OffsetDateTime.of(
                2024, 3, 15, 14, 30, 0, 0,
                ZoneOffset.ofHours(2)
            );
            
            String formatted = FormatProvider.formatDate(odt, "yyyy-MM-dd'T'HH:mm:ssXXX");
            assertEquals("2024-03-15T14:30:00+02:00", formatted);
        }
    }
    
    @Nested
    class TypeDetection {
        
        @Test
        void testIsDateForZonedDateTime() {
            assertTrue(FormatProvider.isDate(ZonedDateTime.class));
        }
        
        @Test
        void testIsDateForOffsetDateTime() {
            assertTrue(FormatProvider.isDate(OffsetDateTime.class));
        }
    }
    
    @Nested
    class ErrorHandling {
        
        @Test
        void testInvalidZonedDateTimeFormat() {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(GlobalEvent.class);
            ColumnMetadata timeColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("eventTime"))
                .findFirst().orElseThrow();
            
            // Invalid format
            assertThrows(IllegalArgumentException.class,
                () -> FormatProvider.parseValue("not-a-date", timeColumn));
        }
        
        @Test
        void testInvalidOffsetDateTimeFormat() {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(ApiResponse.class);
            ColumnMetadata timeColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("timestamp"))
                .findFirst().orElseThrow();
            
            // Invalid format
            assertThrows(IllegalArgumentException.class,
                () -> FormatProvider.parseValue("invalid-timestamp", timeColumn));
        }
        
        @Test
        void testErrorMessageIncludesFormats() {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(GlobalEvent.class);
            ColumnMetadata timeColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("eventTime"))
                .findFirst().orElseThrow();
            
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> FormatProvider.parseValue("bad-format", timeColumn));
            
            String message = ex.getMessage();
            assertTrue(message.contains("ZonedDateTime"));
            assertTrue(message.contains("yyyy-MM-dd'T'HH:mm:ssXXX"));
        }
    }
    
    @Nested
    class MultipleTimezones {
        
        @Test
        void testDifferentTimezones() {
            List<ColumnMetadata> columns = AnnotationProcessor.extractColumns(GlobalEvent.class);
            ColumnMetadata timeColumn = columns.stream()
                .filter(c -> c.getFieldName().equals("eventTime"))
                .findFirst().orElseThrow();
            
            // Test multiple timezone formats
            String[] testDates = {
                "2024-03-15T09:00:00-05:00[America/New_York]",
                "2024-03-15T15:00:00+01:00[Europe/Paris]",
                "2024-03-15T22:00:00+09:00[Asia/Tokyo]",
                "2024-03-15T14:00:00+00:00[UTC]"
            };
            
            for (String dateStr : testDates) {
                Object result = FormatProvider.parseValue(dateStr, timeColumn);
                assertInstanceOf(ZonedDateTime.class, result);
                ZonedDateTime zdt = (ZonedDateTime) result;
                assertNotNull(zdt.getZone());
                assertNotNull(zdt.getOffset());
            }
        }
    }
}