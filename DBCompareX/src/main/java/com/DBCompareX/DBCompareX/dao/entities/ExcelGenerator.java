package com.DBCompareX.DBCompareX.dao.entities;

import com.DBCompareX.DBCompareX.util.NormalizationUtils;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

@Component
public class ExcelGenerator {
    private static final Logger logger = LoggerFactory.getLogger(ExcelGenerator.class);

    // Remove the workbook as a class field
    private CellStyle headerStyle;
    private CellStyle defaultStyle;
    private CellStyle db2DifferenceStyle;
    private CellStyle numericCellStyle;
    private CellStyle dateCellStyle;

    // Constructor injection
    public ExcelGenerator() {
        // Remove initialization here as we'll create a new workbook for each report
    }

    /**
     * Initializes styles for the Excel workbook.
     */
    private void initializeStyles(Workbook workbook) {
        // Header style
        headerStyle = workbook.createCellStyle();
        headerStyle.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
        headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        headerStyle.setBorderBottom(BorderStyle.THIN);
        headerStyle.setBorderTop(BorderStyle.THIN);
        headerStyle.setBorderRight(BorderStyle.THIN);
        headerStyle.setBorderLeft(BorderStyle.THIN);
        Font headerFont = workbook.createFont();
        headerFont.setBold(true);
        headerStyle.setFont(headerFont);

        // Default style
        defaultStyle = workbook.createCellStyle();
        defaultStyle.setBorderBottom(BorderStyle.THIN);
        defaultStyle.setBorderTop(BorderStyle.THIN);
        defaultStyle.setBorderRight(BorderStyle.THIN);
        defaultStyle.setBorderLeft(BorderStyle.THIN);

        // DB2 Difference style (bright red background for differences in DB2 rows)
        db2DifferenceStyle = workbook.createCellStyle();
        db2DifferenceStyle.setFillForegroundColor(IndexedColors.RED.getIndex()); // Brighter red
        db2DifferenceStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        db2DifferenceStyle.setBorderBottom(BorderStyle.THIN);
        db2DifferenceStyle.setBorderTop(BorderStyle.THIN);
        db2DifferenceStyle.setBorderRight(BorderStyle.THIN);
        db2DifferenceStyle.setBorderLeft(BorderStyle.THIN);
        Font whiteFont = workbook.createFont();
        whiteFont.setColor(IndexedColors.WHITE.getIndex());
        whiteFont.setBold(true); // Make text bold for better visibility
        db2DifferenceStyle.setFont(whiteFont);

        // Numeric cell style
        numericCellStyle = workbook.createCellStyle();
        DataFormat format = workbook.createDataFormat();
        numericCellStyle.setDataFormat(format.getFormat("#,##0.00")); // Format numbers with two decimal places

        // Date cell style
        dateCellStyle = workbook.createCellStyle();
        dateCellStyle.setDataFormat(format.getFormat("yyyy-MM-dd HH:mm:ss"));
    }

    /**
     * Generates an Excel report focusing only on differences.
     */
    public File generateExcelReport(Map<String, Object> results, String outputPath, List<TableMapping> tableMappings) {
        String timestamp = String.valueOf(System.currentTimeMillis());
        String fileName = outputPath.contains(".xlsx") ?
                outputPath.replace(".xlsx", "_" + timestamp + ".xlsx") :
                outputPath + "_" + timestamp + ".xlsx";
        File outputFile = new File(fileName);

        // Create a new workbook for each report
        try (Workbook workbook = new XSSFWorkbook()) {
            // Initialize styles for this workbook
            initializeStyles(workbook);

            XSSFSheet sheet = (XSSFSheet) workbook.createSheet("Database Comparison");
            int currentRow = 0;

            // Find primary key column(s) from the table mappings
            Set<String> primaryKeyColumns = new LinkedHashSet<>();
            if (tableMappings != null && !tableMappings.isEmpty()) {
                for (TableMapping mapping : tableMappings) {
                    if (mapping.getKeyColumns() != null) {
                        primaryKeyColumns.addAll(mapping.getKeyColumns());
                    }
                }
            }

            // Extract all column names from the data
            Set<String> allColumns = new LinkedHashSet<>();
            extractColumnsFromData(allColumns, (List<Map<String, Object>>) results.get("differences"));
            allColumns.remove("source_record");
            allColumns.remove("target_record");
            allColumns.remove("table_name");

            // Create final column set with proper ordering
            Set<String> columnSet = new LinkedHashSet<>();
            columnSet.add("Database"); // Always include Database column first
            for (String pk : primaryKeyColumns) {
                if (allColumns.contains(pk)) {
                    columnSet.add(pk);
                    allColumns.remove(pk); // Remove from original set to avoid duplication
                }
            }
            columnSet.addAll(allColumns);

            // Convert to array for easier handling
            String[] headers = columnSet.toArray(new String[0]);

            // Create header row
            Row headerRow = sheet.createRow(currentRow++);
            for (int i = 0; i < headers.length; i++) {
                Cell cell = headerRow.createCell(i);
                cell.setCellValue(headers[i]);
                cell.setCellStyle(headerStyle);
            }

            // Process different records
            processDifferences(sheet, results, headers, primaryKeyColumns, currentRow);

            // Autosize columns
            for (int i = 0; i < headers.length; i++) {
                sheet.autoSizeColumn(i);
            }

            // Write to file
            try (FileOutputStream fileOut = new FileOutputStream(outputFile)) {
                workbook.write(fileOut);
                logger.info("Excel report generated successfully at: {}", outputFile.getAbsolutePath());
            }
        } catch (IOException e) {
            logger.error("Error generating Excel report: {}", e.getMessage());
            throw new RuntimeException("Failed to generate Excel report", e);
        }

        return outputFile;
    }

    /**
     * Extracts all column names from the data.
     */
    private void extractColumnsFromData(Set<String> columns, List<Map<String, Object>> records) {
        if (records != null && !records.isEmpty()) {
            for (Map<String, Object> record : records) {
                Map<String, Object> sourceRecord = (Map<String, Object>) record.get("source_record");
                Map<String, Object> targetRecord = (Map<String, Object>) record.get("target_record");
                if (sourceRecord != null) {
                    columns.addAll(sourceRecord.keySet());
                }
                if (targetRecord != null) {
                    columns.addAll(targetRecord.keySet());
                }
            }
        }
    }

    /**
     * Processes differences and writes them to the Excel sheet.
     */
    private void processDifferences(XSSFSheet sheet, Map<String, Object> results, String[] headers,
                                    Set<String> primaryKeyColumns, int currentRow) {
        List<Map<String, Object>> differences = (List<Map<String, Object>>) results.get("differences");
        if (differences != null && !differences.isEmpty()) {
            for (Map<String, Object> difference : differences) {
                Map<String, Object> sourceRecord = (Map<String, Object>) difference.get("source_record");
                Map<String, Object> targetRecord = (Map<String, Object>) difference.get("target_record");

                if (sourceRecord != null && targetRecord != null) {
                    Row db1Row = sheet.createRow(currentRow++);
                    Row db2Row = sheet.createRow(currentRow++);

                    Cell db1DatabaseCell = db1Row.createCell(0);
                    db1DatabaseCell.setCellValue("DB1");
                    db1DatabaseCell.setCellStyle(defaultStyle);

                    Cell db2DatabaseCell = db2Row.createCell(0);
                    db2DatabaseCell.setCellValue("DB2");
                    db2DatabaseCell.setCellStyle(defaultStyle);

                    for (int j = 1; j < headers.length; j++) {
                        String header = headers[j];
                        boolean isPrimaryKey = primaryKeyColumns.contains(header);

                        // Normalize values using the existing method
                        Object db1Value = NormalizationUtils.normalizeValue(sourceRecord.get(header));
                        Object db2Value = NormalizationUtils.normalizeValue(targetRecord.get(header));

                        // Special handling for specific column types
                        boolean isSemanticallySame = false;
                        if (!Objects.equals(db1Value, db2Value)) {
                            // Handle phone numbers - compare only digits
                            if (header.toLowerCase().contains("phone")) {
                                // Phone handling code remains the same
                                String phone1 = db1Value != null ? db1Value.toString() : "";
                                String phone2 = db2Value != null ? db2Value.toString() : "";

                                phone1 = cleanPhoneNumber(phone1);
                                phone2 = cleanPhoneNumber(phone2);

                                isSemanticallySame = phone1.equals(phone2) && !phone1.isEmpty();
                            }
                            // Handle IDs - try to compare as numbers if possible
                            else if (header.toLowerCase().contains("id")) {
                                // ID handling code remains the same
                                try {
                                    double id1 = Double.parseDouble(db1Value.toString());
                                    double id2 = Double.parseDouble(db2Value.toString());
                                    isSemanticallySame = Math.abs(id1 - id2) < 0.001;
                                } catch (Exception e) {
                                    // If parsing fails, keep isSemanticallySame as false
                                }
                            }
                            // Enhanced date handling - more dynamic for different column types
                            else if (isLikelyDateColumn(header) || isLikelyDateValue(db1Value) || isLikelyDateValue(db2Value)) {
                                String date1 = db1Value != null ? db1Value.toString() : "";
                                String date2 = db2Value != null ? db2Value.toString() : "";

                                // Extract just the date part for comparison
                                date1 = extractDatePart(date1);
                                date2 = extractDatePart(date2);

                                isSemanticallySame = date1.equals(date2) && !date1.isEmpty();
                            }
                        } else {
                            isSemanticallySame = true; // They're already equal
                        }

                        // DB1 row
                        Cell db1Cell = db1Row.createCell(j);
                        if (header.toLowerCase().contains("phone") && db1Value != null) {
                            // Display cleaned phone number for better readability
                            String cleanedPhone = cleanPhoneNumberForDisplay(db1Value.toString());
                            db1Cell.setCellValue(cleanedPhone);
                            db1Cell.setCellStyle(defaultStyle);
                        } else if (db1Value instanceof Number) {
                            db1Cell.setCellValue(((Number) db1Value).doubleValue());
                            db1Cell.setCellStyle(numericCellStyle); // Apply numeric style
                        } else if (db1Value instanceof Date) {
                            db1Cell.setCellValue(((Date) db1Value).getTime());
                            db1Cell.setCellStyle(dateCellStyle); // Apply date style
                        } else {
                            db1Cell.setCellValue(db1Value != null ? db1Value.toString() : "<NULL>");
                            db1Cell.setCellStyle(defaultStyle);
                        }

                        // DB2 row
                        Cell db2Cell = db2Row.createCell(j);
                        if (header.toLowerCase().contains("phone") && db2Value != null) {
                            // Display cleaned phone number for better readability
                            String cleanedPhone = cleanPhoneNumberForDisplay(db2Value.toString());
                            db2Cell.setCellValue(cleanedPhone);
                            db2Cell.setCellStyle(defaultStyle);
                        } else if (db2Value instanceof Number) {
                            db2Cell.setCellValue(((Number) db2Value).doubleValue());
                            db2Cell.setCellStyle(numericCellStyle); // Apply numeric style
                        } else if (db2Value instanceof Date) {
                            db2Cell.setCellValue(((Date) db2Value).getTime());
                            db2Cell.setCellStyle(dateCellStyle); // Apply date style
                        } else {
                            db2Cell.setCellValue(db2Value != null ? db2Value.toString() : "<NULL>");
                            db2Cell.setCellStyle(defaultStyle);
                        }

                        // Apply red style to DB2 cells that differ from DB1 (semantically)
                        if (!isSemanticallySame && !isPrimaryKey) {
                            db2Cell.setCellStyle(db2DifferenceStyle);
                        } else {
                            db2Cell.setCellStyle(defaultStyle);
                        }
                    }

                    // Add empty row for separation
                    sheet.createRow(currentRow++);
                }
            }
        }
    }

    /**
     * Formats a phone number for display by removing date/time parts but keeping formatting.
     */
    private String cleanPhoneNumberForDisplay(String phone) {
        if (phone == null || phone.isEmpty()) {
            return "<NULL>";
        }

        // Remove common date/time patterns that might be appended to phone numbers
        String[] dateTimeParts = {"00:00", " 00:", "T00:", ".000", "+0000"};
        for (String part : dateTimeParts) {
            if (phone.contains(part)) {
                phone = phone.substring(0, phone.indexOf(part)).trim();
            }
        }

        return phone;
    }

    /**
     * Cleans a phone number by removing date/time formatting that might have been applied.
     */
    private String cleanPhoneNumber(String phone) {
        if (phone == null || phone.isEmpty()) {
            return "";
        }

        // Remove common date/time patterns that might be appended to phone numbers
        String[] dateTimeParts = {"00:00", " 00:", "T00:", ".000", "+0000"};
        for (String part : dateTimeParts) {
            if (phone.contains(part)) {
                phone = phone.substring(0, phone.indexOf(part)).trim();
            }
        }

        // Extract only digits for comparison
        return phone.replaceAll("[^0-9]", "");
    }

    /**
     * Checks if a column name is likely to contain date values.
     */
    private boolean isLikelyDateColumn(String columnName) {
        String lowerName = columnName.toLowerCase();
        return lowerName.contains("date") ||
                lowerName.contains("time") ||
                lowerName.contains("created") ||
                lowerName.contains("modified") ||
                lowerName.contains("updated") ||
                lowerName.contains("birth") ||
                lowerName.endsWith("_at") ||
                lowerName.endsWith("_on");
    }

    /**
     * Checks if a value is likely to be a date.
     */
    private boolean isLikelyDateValue(Object value) {
        if (value == null) return false;
        if (value instanceof Date) return true;

        String strValue = value.toString().trim();

        // Check for common date formats
        return strValue.matches("\\d{4}-\\d{2}-\\d{2}.*") || // yyyy-MM-dd
                strValue.matches("\\d{2}/\\d{2}/\\d{4}.*") || // MM/dd/yyyy
                strValue.matches("\\d{2}-\\d{2}-\\d{4}.*") || // dd-MM-yyyy
                strValue.contains("00:00:00") || // Common time part in dates
                strValue.matches(".*\\d{4}[-/]\\d{1,2}[-/]\\d{1,2}.*"); // Various with year
    }

    /**
     * Extracts just the date part from a string that might contain date and time.
     */
    private String extractDatePart(String dateString) {
        if (dateString == null || dateString.isEmpty()) {
            return "";
        }

        // Handle common date formats
        if (dateString.matches("\\d{4}-\\d{2}-\\d{2}.*")) {
            // yyyy-MM-dd format (possibly with time)
            return dateString.substring(0, 10);
        } else if (dateString.matches("\\d{2}/\\d{2}/\\d{4}.*")) {
            // MM/dd/yyyy format
            return dateString.substring(0, 10);
        } else if (dateString.matches("\\d{2}-\\d{2}-\\d{4}.*")) {
            // dd-MM-yyyy format
            return dateString.substring(0, 10);
        }

        // Try to find and extract date patterns
        if (dateString.contains(" 00:00:00")) {
            return dateString.substring(0, dateString.indexOf(" 00:00:00"));
        }

        // For other formats, just return as is
        return dateString;
    }
}