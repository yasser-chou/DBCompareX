package com.DBCompareX.DBCompareX.controller;

import com.DBCompareX.DBCompareX.dao.entities.ComparisonRequest;
import com.DBCompareX.DBCompareX.dao.entities.TableMapping;
import com.DBCompareX.DBCompareX.service.TableSchemaExtractor;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/compare")
public class TableSchemaExtractorController {
    private static final Logger logger = LoggerFactory.getLogger(TableSchemaExtractorController.class);
    private final TableSchemaExtractor tableSchemaExtractor;

    // Constants for response messages
    private static final String STATUS_SUCCESS = "success";
    private static final String STATUS_ERROR = "error";
    private static final String DEFAULT_REPORT_NAME = "database_comparison_report.xlsx";

    @Autowired
    public TableSchemaExtractorController(TableSchemaExtractor tableSchemaExtractor) {
        this.tableSchemaExtractor = tableSchemaExtractor;
    }

    /**
     * Compare all tables between two databases and generate a report
     */
    // Removed unused compareDatabases method

    /**
     * Download the generated report
     */
    @Operation(summary = "Download the generated report",
            description = "Downloads the Excel report generated from database comparison.")
    @ApiResponse(responseCode = "200", description = "File downloaded successfully")
    @ApiResponse(responseCode = "404", description = "File not found")
    @ApiResponse(responseCode = "500", description = "Internal server error during download")
    @GetMapping("/download")
    public ResponseEntity<Resource> downloadReport(@RequestParam String filePath) {
        File file = null;
        FileSystemResource resource = null;
        
        try {
            file = new File(filePath);
            if (!file.exists() || !file.isFile()) {
                logger.warn("File not found or invalid: {}", filePath);
                return ResponseEntity.notFound().build();
            }

            // Try to get exclusive access to the file
            if (!file.renameTo(file)) {
                logger.warn("File is locked by another process: {}", filePath);
                return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(null);
            }

            resource = new FileSystemResource(file);
            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, 
                           "attachment; filename=\"" + file.getName() + "\"")
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .contentLength(file.length())
                    .body(resource);
        } catch (Exception e) {
            logger.error("Error downloading report: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        } finally {
            // Clean up the file after serving
            if (file != null) {
                try {
                    boolean deleted = file.delete();
                    if (!deleted) {
                        // Schedule deletion for when the JVM exits
                        file.deleteOnExit();
                        logger.warn("Could not delete file immediately, scheduled for deletion on JVM exit: {}", filePath);
                    }
                } catch (Exception e) {
                    logger.error("Error deleting file: {}", e.getMessage());
                    // Schedule deletion for when the JVM exits
                    file.deleteOnExit();
                }
            }
        }
    }

    /**
     * Compare selected tables between two databases and generate a report
     */
    @Operation(summary = "Compare selected tables",
            description = "Compares selected tables between two databases and generates a report.")
    @ApiResponse(responseCode = "200", description = "Comparison successful, report generated")
    @ApiResponse(responseCode = "400", description = "Invalid input or no tables selected")
    @ApiResponse(responseCode = "500", description = "Internal server error during comparison")
    @PostMapping("/compare-selected-tables")
    public ResponseEntity<?> compareSelectedTables(@Valid @RequestBody ComparisonRequest request) {
        try {
            List<TableMapping> selectedTables = request.getTableMappings();
            if (selectedTables == null || selectedTables.isEmpty()) {
                return ResponseEntity.badRequest().body(createErrorResponse("No tables selected for comparison"));
            }
            String outputPath = generateOutputPath(DEFAULT_REPORT_NAME);
            File reportFile = tableSchemaExtractor.compareAndGenerateReport(
                    request.getSourceDbType(), request.getTargetDbType(),
                    request.getSourceHost(), request.getSourcePort(), request.getSourceDbName(),
                    request.getSourceUsername(), request.getSourcePassword(),
                    request.getTargetHost(), request.getTargetPort(), request.getTargetDbName(),
                    request.getTargetUsername(), request.getTargetPassword(),
                    outputPath, selectedTables, 
                    request.getSourceSchemaFilter(), request.getTargetSchemaFilter(), request.getMaxTables());
            return handleFileResponse(reportFile, "Selected tables comparison completed successfully");
        } catch (Exception e) {
            logger.error("Error comparing selected tables: ", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Error comparing selected tables: " + e.getMessage()));
        }
    }

    /**
     * Helper method to generate dynamic output path with unique name
     */
    private String generateOutputPath(String fileName) {
        String timestamp = String.valueOf(System.currentTimeMillis());
        String baseName = fileName;
        if (baseName.contains(".")) {
            int dotIndex = baseName.lastIndexOf('.');
            baseName = baseName.substring(0, dotIndex) + "_" + timestamp + baseName.substring(dotIndex);
        } else {
            baseName = baseName + "_" + timestamp;
        }
        return System.getProperty("java.io.tmpdir") + File.separator + baseName;
    }

    /**
     * Helper method to handle file response
     */
    private ResponseEntity<?> handleFileResponse(File file, String successMessage) {
        if (file != null && file.exists()) {
            Map<String, Object> response = createSuccessResponse(successMessage);
            response.put("filePath", file.getAbsolutePath());
            return ResponseEntity.ok(response);
        } else {
            return ResponseEntity.badRequest().body(createErrorResponse("Failed to generate comparison report"));
        }
    }

    /**
     * Helper method to create a success response
     */
    private Map<String, Object> createSuccessResponse(String message) {
        Map<String, Object> response = new HashMap<>();
        response.put("status", STATUS_SUCCESS);
        response.put("message", message);
        return response;
    }

    /**
     * Helper method to create an error response
     */
    private Map<String, Object> createErrorResponse(String message) {
        Map<String, Object> response = new HashMap<>();
        response.put("status", STATUS_ERROR);
        response.put("message", message);
        return response;
    }

    @PostMapping("/get-available-tables")
    public ResponseEntity<?> getAvailableTables(@Valid @RequestBody ComparisonRequest request) {
        try {
            List<TableMapping> tableMappings = tableSchemaExtractor.findCommonTables(
                    request.getSourceDbType(), request.getTargetDbType(),
                    request.getSourceHost(), request.getSourcePort(), request.getSourceDbName(),
                    request.getSourceUsername(), request.getSourcePassword(),
                    request.getTargetHost(), request.getTargetPort(), request.getTargetDbName(),
                    request.getTargetUsername(), request.getTargetPassword(),
                    request.getSourceSchemaFilter(), request.getTargetSchemaFilter(), request.getMaxTables());
            // Transform TableMapping list into Map
            Map<String, List<String>> availableTables = new HashMap<>();
            availableTables.put("commonTables", tableMappings.stream()
                    .map(mapping -> mapping.getSourceTable() + " -> " + mapping.getTargetTable())
                    .collect(Collectors.toList()));
            return ResponseEntity.ok(availableTables);
        } catch (Exception e) {
            logger.error("Error retrieving available tables: ", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Error retrieving available tables: " + e.getMessage()));
        }
    }

    @PostMapping("/execute-query")
    public ResponseEntity<?> executeQuery(@RequestBody Map<String, Object> request) {
        try {
            String dbType = (String) request.get("sourceDbType");
            String host = (String) request.get("sourceHost");
            String port = (String) request.get("sourcePort");
            String dbName = (String) request.get("sourceDbName");
            String username = (String) request.get("sourceUsername");
            String password = (String) request.get("sourcePassword");
            String query = (String) request.get("query");

            List<Map<String, Object>> results = tableSchemaExtractor.executeCustomQuery(
                    dbType, host, Integer.parseInt(port), dbName, username, password, query);

            return ResponseEntity.ok(Map.of("results", results));
        } catch (Exception e) {
            logger.error("Error executing query: ", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }
}