package dataset4j.poi;

/**
 * Converts Java {@link java.time.format.DateTimeFormatter} patterns to
 * Excel custom number-format strings.
 *
 * <p>Most tokens (y, M, d, H, h, m, s) are identical between Java and Excel,
 * but several diverge. This class handles the known differences:
 *
 * <table>
 *   <tr><th>Meaning</th><th>Java</th><th>Excel</th></tr>
 *   <tr><td>AM/PM marker</td><td>{@code a}</td><td>{@code AM/PM}</td></tr>
 *   <tr><td>Day of week (abbrev)</td><td>{@code EEE}</td><td>{@code DDD}</td></tr>
 *   <tr><td>Day of week (full)</td><td>{@code EEEE}</td><td>{@code DDDD}</td></tr>
 *   <tr><td>Fraction of second</td><td>{@code SSS}</td><td>{@code .000}</td></tr>
 * </table>
 *
 * <p>Quoted literal text (single-quote delimited in Java) is preserved as-is
 * since Excel uses the same quoting convention for literal characters.
 */
final class JavaToExcelDateFormat {

    private JavaToExcelDateFormat() {}

    /**
     * Convert a Java date-format pattern to an Excel-compatible format string.
     *
     * @param javaPattern a {@link java.time.format.DateTimeFormatter} pattern
     * @return the equivalent Excel custom number-format string
     */
    static String convert(String javaPattern) {
        if (javaPattern == null || javaPattern.isEmpty()) {
            return javaPattern;
        }

        StringBuilder excel = new StringBuilder(javaPattern.length() + 8);
        int i = 0;
        int len = javaPattern.length();

        while (i < len) {
            char c = javaPattern.charAt(i);

            // Preserve quoted literals (single-quote delimited)
            if (c == '\'') {
                int close = javaPattern.indexOf('\'', i + 1);
                if (close == -1) {
                    // Unterminated quote — copy rest as-is
                    excel.append(javaPattern, i, len);
                    break;
                }
                excel.append(javaPattern, i, close + 1);
                i = close + 1;
                continue;
            }

            // AM/PM marker: Java 'a' → Excel 'AM/PM'
            if (c == 'a') {
                excel.append("AM/PM");
                i++;
                continue;
            }

            // Day-of-week: Java E/EE/EEE → Excel DDD, EEEE+ → DDDD
            if (c == 'E') {
                int count = countRun(javaPattern, i, 'E');
                excel.append(count >= 4 ? "DDDD" : "DDD");
                i += count;
                continue;
            }

            // Fraction-of-second: Java S+ → Excel .0+ (with leading dot)
            if (c == 'S') {
                int count = countRun(javaPattern, i, 'S');
                // Only prepend dot if previous char isn't already a dot
                if (excel.length() == 0 || excel.charAt(excel.length() - 1) != '.') {
                    excel.append('.');
                }
                excel.append("0".repeat(count));
                i += count;
                continue;
            }

            // Pass through all other characters (y, M, d, H, h, m, s, literals, separators)
            excel.append(c);
            i++;
        }

        return excel.toString();
    }

    private static int countRun(String s, int start, char c) {
        int count = 0;
        while (start + count < s.length() && s.charAt(start + count) == c) {
            count++;
        }
        return count;
    }
}
