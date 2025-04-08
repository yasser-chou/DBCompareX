package com.DBCompareX.DBCompareX.dao.entities;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

@Component
public class ExcelGenerator {
    private static final Logger logger = LoggerFactory.getLogger(ExcelGenerator.class);
    private CellStyle headerStyle;
    private CellStyle defaultStyle;
    private CellStyle db2DifferenceStyle;

    private void initializeStyles(XSSFWorkbook workbook) {
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
        db2DifferenceStyle.setFillForegroundColor(IndexedColors.RED1.getIndex()); // Brighter red
        db2DifferenceStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        db2DifferenceStyle.setBorderBottom(BorderStyle.THIN);
        db2DifferenceStyle.setBorderTop(BorderStyle.THIN);
        db2DifferenceStyle.setBorderRight(BorderStyle.THIN);
        db2DifferenceStyle.setBorderLeft(BorderStyle.THIN);
        Font whiteFont = workbook.createFont();
        whiteFont.setColor(IndexedColors.WHITE.getIndex());
        whiteFont.setBold(true); // Make text bold for better visibility
        db2DifferenceStyle.setFont(whiteFont);
    }

    /**
     * Generates an Excel report focusing only on differences
     */
    public File generateExcelReport(Map<String, Object> results, String outputPath, List<TableMapping> tableMappings) {
        String timestamp = String.valueOf(System.currentTimeMillis());
        String fileName = outputPath.contains(".xlsx") ?
                outputPath.replace(".xlsx", "_" + timestamp + ".xlsx") :
                outputPath + "_" + timestamp + ".xlsx";
        File outputFile = new File(fileName);
        try (XSSFWorkbook workbook = new XSSFWorkbook()) {
            initializeStyles(workbook);
            XSSFSheet sheet = workbook.createSheet("Database Comparison");
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

    private void extractColumnsFromData(Set<String> columns, List<Map<String, Object>> records) {
        if (records != null && !records.isEmpty()) {
            for (Map<String, Object> record : records) {
                // Include all columns from source_record and target_record
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

    private void processDifferences(XSSFSheet sheet, Map<String, Object> results, String[] headers,
                                    Set<String> primaryKeyColumns, int currentRow) {
        List<Map<String, Object>> differences = (List<Map<String, Object>>) results.get("differences");
        if (differences != null && !differences.isEmpty()) {
            // Process each pair of records (one from DB1, one from DB2)
            for (Map<String, Object> difference : differences) {
                // Extract source and target records
                Map<String, Object> sourceRecord = (Map<String, Object>) difference.get("source_record");
                Map<String, Object> targetRecord = (Map<String, Object>) difference.get("target_record");
                
                if (sourceRecord != null && targetRecord != null) {
                    // Create rows for DB1 and DB2
                    Row db1Row = sheet.createRow(currentRow++);
                    Row db2Row = sheet.createRow(currentRow++);

                    // Set the Database column explicitly
                    Cell db1DatabaseCell = db1Row.createCell(0);
                    db1DatabaseCell.setCellValue("DB1");
                    db1DatabaseCell.setCellStyle(defaultStyle);

                    Cell db2DatabaseCell = db2Row.createCell(0);
                    db2DatabaseCell.setCellValue("DB2");
                    db2DatabaseCell.setCellStyle(defaultStyle);

                    // Fill data for both rows
                    for (int j = 1; j < headers.length; j++) {
                        String header = headers[j];
                        boolean isPrimaryKey = primaryKeyColumns.contains(header);

                        // DB1 row
                        Cell db1Cell = db1Row.createCell(j);
                        Object db1Value = sourceRecord.get(header);
                        db1Cell.setCellValue(db1Value != null ? db1Value.toString() : "<NULL>");
                        db1Cell.setCellStyle(defaultStyle);

                        // DB2 row
                        Cell db2Cell = db2Row.createCell(j);
                        Object db2Value = targetRecord.get(header);
                        db2Cell.setCellValue(db2Value != null ? db2Value.toString() : "<NULL>");

                        // Apply red style to DB2 cells that differ from DB1
                        if (!Objects.equals(db1Value, db2Value) && !isPrimaryKey) {
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
}