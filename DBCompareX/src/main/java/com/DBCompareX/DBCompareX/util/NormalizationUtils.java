package com.DBCompareX.DBCompareX.util;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;

public class NormalizationUtils {
    
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final SimpleDateFormat[] DATE_PARSERS = {
        new SimpleDateFormat("yyyy-MM-dd"),
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"),
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"),
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"),
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    };

    // Pattern to identify phone number-like strings
    private static final Pattern PHONE_PATTERN = Pattern.compile("^\\d{3,4}-\\d{2}-\\d{2}.*$");
    
    public static Object normalizeValue(Object value) {
        if (value == null) {
            return null;
        }
        
        // Handle phone numbers
        if (value instanceof String) {
            String strValue = ((String) value).trim();
            if (PHONE_PATTERN.matcher(strValue).matches()) {
                return strValue; // Return phone numbers as-is
            }
        }
        
        // Handle different date/time types
        if (value instanceof Date || value instanceof Timestamp || 
            value instanceof LocalDate || value instanceof LocalDateTime ||
            value instanceof String) {
            return normalizeDateTime(value);
        }
        
        // For non-date values, return as is
        return value;
    }
    
    private static String normalizeDateTime(Object value) {
        try {
            if (value instanceof Date) {
                return ((Date) value).toLocalDate().format(DATE_FORMATTER);
            } else if (value instanceof Timestamp) {
                String timestampStr = ((Timestamp) value).toString();
                // Remove any trailing zeros in the fractional seconds
                timestampStr = timestampStr.replaceAll("\\.?0+$", "");
                return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Timestamp.valueOf(timestampStr));
            } else if (value instanceof LocalDate) {
                return ((LocalDate) value).format(DATE_FORMATTER);
            } else if (value instanceof LocalDateTime) {
                return ((LocalDateTime) value).format(TIMESTAMP_FORMATTER);
            } else if (value instanceof String) {
                String strValue = ((String) value).trim();
                
                // Try parsing with DateTimeFormatter first
                try {
                    LocalDate date = LocalDate.parse(strValue);
                    return date.format(DATE_FORMATTER);
                } catch (DateTimeParseException e) {
                    try {
                        LocalDateTime dateTime = LocalDateTime.parse(strValue);
                        return dateTime.format(TIMESTAMP_FORMATTER);
                    } catch (DateTimeParseException e2) {
                        // If DateTimeFormatter fails, try SimpleDateFormat
                        for (SimpleDateFormat parser : DATE_PARSERS) {
                            try {
                                java.util.Date parsedDate = parser.parse(strValue);
                                // Remove fractional seconds for consistent comparison
                                String dateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(parsedDate);
                                return dateStr.replaceAll("\\.?0+$", "");
                            } catch (ParseException e3) {
                                // Continue to next parser
                                continue;
                            }
                        }
                        // If all parsing fails, return original string
                        return strValue;
                    }
                }
            }
        } catch (Exception e) {
            // If any error occurs during normalization, return the original value as string
            return value.toString();
        }
        
        return value.toString();
    }
} 