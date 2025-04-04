package com.DBCompareX.DBCompareX.dao.entities;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;


@Component
public class ExcelGenerator {

    private static final Logger logger = LoggerFactory.getLogger(ExcelGenerator.class);
    
    // Define column names for the report
    private static final String[] COLUMN_NAMES = {"id", "first_name", "last_name", "age", "salary", "email", "city"};

    public void generateComparisonReport(Map<String, List<Map<String, Object>>> results, String outputPath) {
        try (Workbook workbook = new XSSFWorkbook()) {
            // Create sheets for each section
            String[] sections = {"differences", "unmatched_source", "unmatched_target", "identical"};
            for (String section : sections) {
                Sheet sheet = workbook.createSheet(section);
                List<Map<String, Object>> data = results.get(section);

                if (data != null && !data.isEmpty()) {
                    // Write headers dynamically
                    Row headerRow = sheet.createRow(0);
                    Set<String> headers = data.get(0).keySet();
                    int colIndex = 0;
                    for (String header : headers) {
                        Cell cell = headerRow.createCell(colIndex++);
                        cell.setCellValue(header);
                    }

                    // Write data rows
                    int rowIndex = 1;
                    for (Map<String, Object> row : data) {
                        Row dataRow = sheet.createRow(rowIndex++);
                        colIndex = 0;
                        for (String header : headers) {
                            Cell cell = dataRow.createCell(colIndex++);
                            Object value = row.get(header);
                            if (value instanceof String) {
                                cell.setCellValue((String) value);
                            } else if (value instanceof Number) {
                                cell.setCellValue(((Number) value).doubleValue());
                            } else {
                                cell.setCellValue(value != null ? value.toString() : "");
                            }
                        }
                    }
                }
            }

            // Save the workbook to the output path
            try (FileOutputStream fileOut = new FileOutputStream(outputPath)) {
                workbook.write(fileOut);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error generating Excel report: " + e.getMessage(), e);
        }
    }

    private CellStyle createHeaderStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        Font font = workbook.createFont();
        font.setBold(true);
        style.setFont(font);
        style.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        return style;
    }
    
    private CellStyle createSectionHeaderStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        Font font = workbook.createFont();
        font.setBold(true);
        style.setFont(font);
        style.setFillForegroundColor(IndexedColors.LIGHT_YELLOW.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        return style;
    }

    private CellStyle createRedFillStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        style.setFillForegroundColor(IndexedColors.RED.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        return style;
    }

    private int writeDifferences(Sheet sheet, List<Map<String, Object>> differences,
                                 CellStyle sectionHeaderStyle, CellStyle redFillStyle, int startRow) {
        if (differences == null || differences.isEmpty()) {
            return startRow;
        }

        // Add section header
        Row headerRow = sheet.createRow(startRow++);
        Cell headerCell = headerRow.createCell(0);
        headerCell.setCellValue("Differences");
        headerCell.setCellStyle(sectionHeaderStyle);

        for (Map<String, Object> diff : differences) {
            // Validate source and target rows
            Map<String, Object> srcRow = (Map<String, Object>) diff.get("src_row");
            Map<String, Object> tgtRow = (Map<String, Object>) diff.get("tgt_row");
            if (srcRow == null || tgtRow == null) {
                logger.warn("Skipping invalid difference entry: srcRow or tgtRow is null");
                continue;
            }

            // Write source row
            Row sourceRow = sheet.createRow(startRow++);
            sourceRow.createCell(0).setCellValue("DB1");
            writeRow(sourceRow, srcRow, 1, null);

            // Write target row with highlighting for differences
            Row targetRow = sheet.createRow(startRow++);
            targetRow.createCell(0).setCellValue("DB2");

            // Write each column and highlight differences
            Map<String, Map<String, Object>> differingFields =
                    (Map<String, Map<String, Object>>) diff.get("differing_fields");
            for (int i = 0; i < COLUMN_NAMES.length; i++) {
                String columnName = COLUMN_NAMES[i];
                Cell cell = targetRow.createCell(i + 1);

                // Get values from both rows
                Object srcValue = srcRow.get(columnName);
                Object tgtValue = tgtRow.get(columnName);

                // Set the target value
                cell.setCellValue(tgtValue != null ? tgtValue.toString() : "");

                // Check if this field is different
                if (differingFields != null && differingFields.containsKey(columnName)) {
                    cell.setCellStyle(redFillStyle);
                }
            }

            startRow++; // Add empty row between differences
        }

        return startRow;
    }

    private int writeUnmatchedRecords(Sheet sheet, List<Map<String, Object>> records,
                                      String section, CellStyle sectionHeaderStyle, int startRow) {
        if (records == null || records.isEmpty()) return startRow;

        // Add section header
        Row headerRow = sheet.createRow(startRow++);
        Cell headerCell = headerRow.createCell(0);
        headerCell.setCellValue(section);
        headerCell.setCellStyle(sectionHeaderStyle);

        for (Map<String, Object> record : records) {
            Row row = sheet.createRow(startRow++);
            row.createCell(0).setCellValue(section.contains("DB1") ? "DB1" : "DB2");
            writeRow(row, record, 1, null);
        }

        return startRow;
    }

    private int writeIdenticalRecords(Sheet sheet, List<Map<String, Object>> records,
                                      CellStyle sectionHeaderStyle, int startRow) {
        if (records == null || records.isEmpty()) return startRow;

        // Add section header
        Row headerRow = sheet.createRow(startRow++);
        Cell headerCell = headerRow.createCell(0);
        headerCell.setCellValue("Identical Records");
        headerCell.setCellStyle(sectionHeaderStyle);

        for (Map<String, Object> record : records) {
            // Write DB1 row
            Row db1Row = sheet.createRow(startRow++);
            db1Row.createCell(0).setCellValue("DB1");
            writeRow(db1Row, record, 1, null);
            
            // Write DB2 row
            Row db2Row = sheet.createRow(startRow++);
            db2Row.createCell(0).setCellValue("DB2");
            writeRow(db2Row, record, 1, null);
        }

        return startRow;
    }

    private void writeRow(Row row, Map<String, Object> data, int startCol, CellStyle style) {
        if (row == null) {
            throw new IllegalArgumentException("Row cannot be null");
        }
        if (data == null) {
            throw new IllegalArgumentException("Data cannot be null");
        }

        for (int i = 0; i < COLUMN_NAMES.length; i++) {
            String columnName = COLUMN_NAMES[i];
            Cell cell = row.createCell(startCol + i);

            // Retrieve the value for the column name, defaulting to an empty string if null
            Object value = data.get(columnName);
            cell.setCellValue(value != null ? value.toString() : "");

            // Apply the cell style if provided
            if (style != null) {
                cell.setCellStyle(style);
            }
        }
    }

    private int addSeparator(Sheet sheet, int rowNum) {
        sheet.createRow(rowNum++);
        return rowNum;
    }
}
