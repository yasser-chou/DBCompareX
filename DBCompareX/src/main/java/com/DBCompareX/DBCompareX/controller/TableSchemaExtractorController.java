package com.DBCompareX.DBCompareX.controller;

import com.DBCompareX.DBCompareX.dao.entities.ComparisonRequest;
import com.DBCompareX.DBCompareX.dao.entities.DatabaseConnector;
import com.DBCompareX.DBCompareX.dao.entities.ExcelGenerator;
import com.DBCompareX.DBCompareX.service.TableSchemaExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

@RestController
@RequestMapping("/api/schema")
public class TableSchemaExtractorController {

     private static final Logger logger = LoggerFactory.getLogger(TableSchemaExtractorController.class);
    private final TableSchemaExtractor tableSchemaExtractor;
    private final DatabaseConnector databaseConnector;

    @Autowired
    public TableSchemaExtractorController(TableSchemaExtractor tableSchemaExtractor, DatabaseConnector databaseConnector) {
        this.tableSchemaExtractor = tableSchemaExtractor;
        this.databaseConnector = databaseConnector;
    }

    /**
     * Fetch all tables from a specific database.
     */
//    @GetMapping("/tables")
//    public ResponseEntity<List<String>> getAllTables(
//            @RequestParam String dbType,
//            @RequestParam String host,
//            @RequestParam int port,
//            @RequestParam String dbName,
//            @RequestParam String username,
//            @RequestParam String password) {
//
//        try {
//            List<String> tables = tableSchemaExtractor.getAllTables(dbType, host, port, dbName, username, password);
//            return ResponseEntity.ok(tables);
//        } catch (Exception e) {
//            return ResponseEntity.internalServerError().body(Collections.emptyList());
//        }
//    }

    /**
     * Fetch paginated data from a specific table in a database.
     */
//    @GetMapping("/data")
//    public ResponseEntity<List<Map<String, Object>>> getTableData(
//            @RequestParam String dbType,
//            @RequestParam String host,
//            @RequestParam int port,
//            @RequestParam String dbName,
//            @RequestParam String username,
//            @RequestParam String password,
//            @RequestParam String tableName,
//            @RequestParam(defaultValue = "100") int limit,
//            @RequestParam(defaultValue = "0") int offset) {
//
//        try {
//            List<Map<String, Object>> data = tableSchemaExtractor.getTableData(
//                    dbType, host, port, dbName, username, password, tableName, limit, offset);
//            return ResponseEntity.ok(data);
//        } catch (Exception e) {
//            return ResponseEntity.internalServerError().body(Collections.emptyList());
//        }
//    }

    /**
     * Fetch data from both source and target databases for comparison.
     */

    @PostMapping("/tables")
    public ResponseEntity<?> compareTables(@RequestBody ComparisonRequest request) {
        try {
            logger.info("Received comparison request for tables: {}", request.getTableMappings());

            Map<String, Object> results = tableSchemaExtractor.fetchDataFromBothDatabases(
                    request.getSourceDbType(),
                    request.getTargetDbType(),
                    request.getSourceHost(),
                    request.getSourcePort(),
                    request.getSourceDbName(),
                    request.getSourceUsername(),
                    request.getSourcePassword(),
                    request.getTargetHost(),
                    request.getTargetPort(),
                    request.getTargetDbName(),
                    request.getTargetUsername(),
                    request.getTargetPassword(),
                    request.getTableMappings()
            );

            return ResponseEntity.ok(results);

        } catch (Exception e) {
            logger.error("Error during table comparison: ", e);
            return ResponseEntity.badRequest()
                    .body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/tables/download")
    public ResponseEntity<Resource> compareAndDownloadExcel(@RequestBody ComparisonRequest request) {
        File file = null;
        try {
            logger.info("Received comparison request with Excel download for tables: {}", request.getTableMappings());
            
            // Generate a unique filename for the Excel report
            String filename = "comparison_report_" + UUID.randomUUID().toString() + ".xlsx";
            String outputPath = System.getProperty("java.io.tmpdir") + File.separator + filename;
            
            // Perform the comparison
            Map<String, Object> comparisonResults = tableSchemaExtractor.fetchDataFromBothDatabases(
                    request.getSourceDbType(),
                    request.getTargetDbType(),
                    request.getSourceHost(),
                    request.getSourcePort(),
                    request.getSourceDbName(),
                    request.getSourceUsername(),
                    request.getSourcePassword(),
                    request.getTargetHost(),
                    request.getTargetPort(),
                    request.getTargetDbName(),
                    request.getTargetUsername(),
                    request.getTargetPassword(),
                    request.getTableMappings()
            );
            
            // Generate the Excel report
            String excelPath = tableSchemaExtractor.generateExcelReport(comparisonResults, outputPath);
            
            // Create the response
            file = new File(excelPath);
            Resource resource = new FileSystemResource(file);
            
            // Schedule file deletion after response is sent
            file.deleteOnExit();
            
            return ResponseEntity.ok()
                    .contentType(MediaType.parseMediaType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
                    .body(resource);
        } catch (Exception e) {
            logger.error("Error generating Excel report: ", e);
            // Clean up the file if it exists
            if (file != null && file.exists()) {
                file.delete();
            }
            throw new RuntimeException("Failed to generate Excel report: " + e.getMessage(), e);
        }
    }
}
