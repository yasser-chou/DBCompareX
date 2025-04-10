package com.DBCompareX.DBCompareX.service;

import com.DBCompareX.DBCompareX.config.DatabaseConfig;
import com.DBCompareX.DBCompareX.util.NormalizationUtils;
import com.DBCompareX.DBCompareX.dao.entities.ExcelGenerator;
import com.DBCompareX.DBCompareX.dao.entities.TableMapping;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.math.BigDecimal;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.stream.Collectors;

@Service
public class TableSchemaExtractor {
    private static final Logger logger = LoggerFactory.getLogger(TableSchemaExtractor.class);

    private final DatabaseConfig databaseConfig;
//    private final SparkSession sparkSession;
    private final ExcelGenerator excelGenerator;

    @Autowired
    public TableSchemaExtractor(DatabaseConfig databaseConfig, ExcelGenerator excelGenerator) {
        this.databaseConfig = databaseConfig;
//        this.sparkSession = sparkSession;
        this.excelGenerator = excelGenerator;

        // Validate configuration on startup
        if (this.databaseConfig == null) {
            throw new IllegalStateException("DatabaseConfig is null. Check your Spring configuration.");
        }
        if (this.databaseConfig.getJdbcUrl() == null || this.databaseConfig.getJdbcUrl().isEmpty()) {
            throw new IllegalStateException("JDBC URL configuration is missing or empty. Check your application.properties file.");
        }
        if (this.databaseConfig.getDriver() == null || this.databaseConfig.getDriver().isEmpty()) {
            throw new IllegalStateException("Driver configuration is missing or empty. Check your application.properties file.");
        }

        logger.info("DatabaseConfig initialized with JDBC URLs: {}", this.databaseConfig.getJdbcUrl());
        logger.info("DatabaseConfig initialized with Drivers: {}", this.databaseConfig.getDriver());
    }



    // Class to represent table metadata
    private static class TableMetadata {
        private final String tableName;
        private final List<String> primaryKeyColumns;
        private final List<String> allColumns;

        public TableMetadata(String tableName, List<String> primaryKeyColumns, List<String> allColumns) {
            this.tableName = tableName;
            this.primaryKeyColumns = primaryKeyColumns;
            this.allColumns = allColumns;
        }

        public String getTableName() {
            return tableName;
        }

        public List<String> getPrimaryKeyColumns() {
            return primaryKeyColumns;
        }

        public List<String> getAllColumns() {
            return allColumns;
        }
    }

    /**
     * Fetch all table names from a database
     */
    public List<String> fetchTableNames(String dbType, String host, int port,
                                        String dbName, String username, String password) {
        String jdbcUrl = getJdbcUrl(dbType, host, port, dbName);
        List<String> tableNames = new ArrayList<>();
        try {
            testConnection(jdbcUrl, username, password);
            try (Connection conn = getConnection(jdbcUrl, username, password)) {
                DatabaseMetaData metaData = conn.getMetaData();
                try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
                    while (tables.next()) {
                        tableNames.add(tables.getString("TABLE_NAME").toLowerCase());
                    }
                }
                if (tableNames.isEmpty()) {
                    try (ResultSet tables = metaData.getTables(null, username.toUpperCase(), "%", new String[]{"TABLE"})) {
                        while (tables.next()) {
                            tableNames.add(tables.getString("TABLE_NAME").toLowerCase());
                        }
                    }
                }
                logger.info("Found {} tables in database {}", tableNames.size(), dbName);
                return tableNames;
            }
        } catch (SQLException e) {
            logger.error("Error fetching table names for database {}: {}", dbName, e.getMessage());
            throw new RuntimeException("Failed to fetch table names: " + e.getMessage(), e);
        }
    }

    /**
     * Test database connection before performing operations
     */
    private void testConnection(String jdbcUrl, String username, String password) throws SQLException {
        logger.info("Testing connection to: {}", jdbcUrl);
        try (Connection conn = getConnection(jdbcUrl, username, password)) {
            if (!conn.isValid(5)) { // 5-second timeout
                throw new SQLException("Connection test failed - connection is invalid");
            }
            logger.info("Connection test successful");
        } catch (SQLException e) {
            logger.error("Connection test failed: {}", e.getMessage());
            throw new SQLException("Failed to connect to database: " + e.getMessage(), e);
        }
    }

    /**
     * Fetch table metadata including primary keys and all columns
     */
    public TableMetadata fetchTableMetadata(String dbType, String host, int port,
                                            String dbName, String username, String password,
                                            String tableName) {
        String jdbcUrl = getJdbcUrl(dbType, host, port, dbName);
        List<String> primaryKeyColumns = new ArrayList<>();
        List<String> allColumns = new ArrayList<>();
        try {
            testConnection(jdbcUrl, username, password);
            try (Connection conn = getConnection(jdbcUrl, username, password)) {
                DatabaseMetaData metaData = conn.getMetaData();
                try (ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, tableName)) {
                    while (primaryKeys.next()) {
                        primaryKeyColumns.add(primaryKeys.getString("COLUMN_NAME").toLowerCase());
                    }
                }
                if (primaryKeyColumns.isEmpty()) {
                    try (ResultSet primaryKeys = metaData.getPrimaryKeys(null, username.toUpperCase(), tableName)) {
                        while (primaryKeys.next()) {
                            primaryKeyColumns.add(primaryKeys.getString("COLUMN_NAME").toLowerCase());
                        }
                    }
                }
                try (ResultSet columns = metaData.getColumns(null, null, tableName, "%")) {
                    while (columns.next()) {
                        allColumns.add(columns.getString("COLUMN_NAME").toLowerCase());
                    }
                }
                if (allColumns.isEmpty()) {
                    try (ResultSet columns = metaData.getColumns(null, username.toUpperCase(), tableName, "%")) {
                        while (columns.next()) {
                            allColumns.add(columns.getString("COLUMN_NAME").toLowerCase());
                        }
                    }
                }
                logger.info("Table {} has {} columns with {} primary keys",
                        tableName, allColumns.size(), primaryKeyColumns.size());
                return new TableMetadata(tableName, primaryKeyColumns, allColumns);
            }
        } catch (SQLException e) {
            logger.error("Error fetching table metadata for table {}: {}", tableName, e.getMessage());
            throw new RuntimeException("Failed to fetch table metadata: " + e.getMessage(), e);
        }
    }

    /**
     * Find common tables between two databases with improved matching
     */
    public List<TableMapping> findCommonTables(
            String srcDbType, String tgtDbType,
            String srcHost, int srcPort, String srcDbName, String srcUsername, String srcPassword,
            String tgtHost, int tgtPort, String tgtDbName, String tgtUsername, String tgtPassword) {

        List<String> srcTables = fetchTableNames(srcDbType, srcHost, srcPort, srcDbName, srcUsername, srcPassword);
        List<String> tgtTables = fetchTableNames(tgtDbType, tgtHost, tgtPort, tgtDbName, tgtUsername, tgtPassword);

        // First try exact matches
        Map<String, String> tgtTablesMap = tgtTables.stream()
                .collect(Collectors.toMap(String::toLowerCase, t -> t, (v1, v2) -> v1));

        List<TableMapping> commonTables = new ArrayList<>();
        Set<String> matchedTargetTables = new HashSet<>();

        // First pass: exact matches
        for (String srcTable : srcTables) {
            String srcTableLower = srcTable.toLowerCase();
            if (tgtTablesMap.containsKey(srcTableLower)) {
                String tgtTable = tgtTablesMap.get(srcTableLower);
                matchedTargetTables.add(tgtTable.toLowerCase());

                TableMetadata srcMetadata = fetchTableMetadata(srcDbType, srcHost, srcPort, srcDbName, srcUsername, srcPassword, srcTable);
                TableMetadata tgtMetadata = fetchTableMetadata(tgtDbType, tgtHost, tgtPort, tgtDbName, tgtUsername, tgtPassword, tgtTable);

                TableMapping mapping = createTableMapping(srcTable, tgtTable, srcMetadata, tgtMetadata,
                        srcDbType, srcHost, srcPort, srcDbName, srcUsername, srcPassword,
                        tgtDbType, tgtHost, tgtPort, tgtDbName, tgtUsername, tgtPassword);
                commonTables.add(mapping);
                logger.info("Found exact match table: {} (source) and {} (target) with key columns: {}",
                        srcTable, tgtTable, mapping.getKeyColumns());
            }
        }

        // Second pass: fuzzy matches for remaining tables (optional)
        // This could be implemented to match tables with different names but similar structure

        return commonTables;
    }

    /**
     * Identifies primary keys and unique constraints for a table
     */
    private List<String> identifyPrimaryKeys(String dbType, String host, int port, String dbName,
                                           String username, String password, String tableName) {
        List<String> primaryKeys = new ArrayList<>();
        Connection conn = null;
        try {
            conn = getConnection(dbType, host, port, dbName, username, password);
            DatabaseMetaData metaData = conn.getMetaData();

            logger.info("Attempting to identify primary keys for table: {}", tableName);

            // First try to get primary keys
            try (ResultSet pkRs = metaData.getPrimaryKeys(null, null, tableName)) {
                while (pkRs.next()) {
                    String columnName = pkRs.getString("COLUMN_NAME");
                    primaryKeys.add(columnName.toLowerCase());
                    logger.info("Found primary key column: {}", columnName);
                }
            }

            // If no primary keys found, look for unique indices
            if (primaryKeys.isEmpty()) {
                logger.info("No primary keys found, checking unique indices for table: {}", tableName);
                try (ResultSet indexRs = metaData.getIndexInfo(null, null, tableName, true, false)) {
                    while (indexRs.next()) {
                        String columnName = indexRs.getString("COLUMN_NAME");
                        if (!primaryKeys.contains(columnName.toLowerCase())) {
                            primaryKeys.add(columnName.toLowerCase());
                            logger.info("Found unique index column: {}", columnName);
                        }
                    }
                }
            }

        } catch (SQLException e) {
            logger.error("Error identifying primary keys for table {}: {}", tableName, e.getMessage());
        } finally {
            closeConnection(conn);
        }

        if (primaryKeys.isEmpty()) {
            logger.warn("No primary or unique keys found for table: {}. Will try business keys.", tableName);
        } else {
            logger.info("Identified key columns for table {}: {}", tableName, primaryKeys);
        }

        return primaryKeys;
    }

    /**
     * Identifies potential business keys based on column properties
     */
    private List<String> identifyBusinessKeys(String dbType, String host, int port, String dbName,
                                            String username, String password, String tableName) {
        List<String> businessKeys = new ArrayList<>();
        Connection conn = null;

        try {
            conn = getConnection(dbType, host, port, dbName, username, password);
            DatabaseMetaData metaData = conn.getMetaData();

            logger.debug("Analyzing columns for business keys in table: {}", tableName);

            // Get column metadata
            try (ResultSet columns = metaData.getColumns(null, null, tableName, null)) {
                while (columns.next()) {
                    String columnName = columns.getString("COLUMN_NAME").toLowerCase();
                    int nullable = columns.getInt("NULLABLE");
                    String typeName = columns.getString("TYPE_NAME").toLowerCase();

                    // Check for common business key patterns
                    if (isBusinessKeyCandidate(columnName, nullable)) {
                        businessKeys.add(columnName);
                        logger.info("Identified business key for table {}: {} (Type: {}, Nullable: {})",
                            tableName, columnName, typeName, nullable == DatabaseMetaData.columnNoNulls ? "NO" : "YES");
                    }
                }
            }

        } catch (SQLException e) {
            logger.error("Error identifying business keys for table {}: {}", tableName, e.getMessage());
        } finally {
            closeConnection(conn);
        }

        return businessKeys;
    }

    /**
     * Checks if a column is a potential business key
     */
    private boolean isBusinessKeyCandidate(String columnName, int nullable) {
        // Common patterns for business keys
        String[] patterns = {
            "_id$", "id$", "_code$", "_number$", "reference", "email",
            "^uuid", "^guid", "unique", "key$", "identifier$"
        };

        // Check if column name matches any business key pattern
        boolean matchesPattern = false;
        for (String pattern : patterns) {
            if (columnName.matches(".*" + pattern + ".*")) {
                logger.debug("Column {} matches business key pattern: {}", columnName, pattern);
                matchesPattern = true;
                break;
            }
        }

        // Column should match pattern and preferably be NOT NULL
        boolean isCandidate = matchesPattern && nullable == DatabaseMetaData.columnNoNulls;
        if (isCandidate) {
            logger.debug("Column {} is a business key candidate", columnName);
        }

        return isCandidate;
    }

    /**
     * Create table mapping with appropriate key columns
     */
    private TableMapping createTableMapping(String srcTable, String tgtTable,
                                           TableMetadata srcMetadata, TableMetadata tgtMetadata,
                                           String srcDbType, String srcHost, int srcPort, String srcDbName,
                                           String srcUsername, String srcPassword,
                                           String tgtDbType, String tgtHost, int tgtPort, String tgtDbName,
                                           String tgtUsername, String tgtPassword) {
        TableMapping mapping = new TableMapping(srcTable, tgtTable);

        // Set database connection details
        mapping.setSourceDbType(srcDbType);
        mapping.setSourceHost(srcHost);
        mapping.setSourcePort(srcPort);
        mapping.setSourceDbName(srcDbName);
        mapping.setSourceUsername(srcUsername);
        mapping.setSourcePassword(srcPassword);

        mapping.setTargetDbType(tgtDbType);
        mapping.setTargetHost(tgtHost);
        mapping.setTargetPort(tgtPort);
        mapping.setTargetDbName(tgtDbName);
        mapping.setTargetUsername(tgtUsername);
        mapping.setTargetPassword(tgtPassword);

        List<String> keyColumns = new ArrayList<>();

        // First try to detect primary keys
        List<String> srcPrimaryKeys = identifyPrimaryKeys(srcDbType, srcHost, srcPort, srcDbName, srcUsername, srcPassword, srcTable);
        List<String> tgtPrimaryKeys = identifyPrimaryKeys(tgtDbType, tgtHost, tgtPort, tgtDbName, tgtUsername, tgtPassword, tgtTable);

        // Use primary keys if available
        if (!srcPrimaryKeys.isEmpty() && !tgtPrimaryKeys.isEmpty()) {
            Set<String> commonPrimaryKeys = new HashSet<>();
            for (String srcPk : srcPrimaryKeys) {
                for (String tgtPk : tgtPrimaryKeys) {
                    if (srcPk.equalsIgnoreCase(tgtPk)) {
                        commonPrimaryKeys.add(srcPk.toLowerCase());
                    }
                }
            }
            keyColumns.addAll(commonPrimaryKeys);
            logger.info("Using primary keys for table mapping: {}", keyColumns);
        }

        // If no common primary keys, try to identify business keys
        if (keyColumns.isEmpty()) {
            List<String> srcBusinessKeys = identifyBusinessKeys(srcDbType, srcHost, srcPort, srcDbName, srcUsername, srcPassword, srcTable);
            List<String> tgtBusinessKeys = identifyBusinessKeys(tgtDbType, tgtHost, tgtPort, tgtDbName, tgtUsername, tgtPassword, tgtTable);

            Set<String> commonBusinessKeys = new HashSet<>();
            for (String srcKey : srcBusinessKeys) {
                for (String tgtKey : tgtBusinessKeys) {
                    if (srcKey.equalsIgnoreCase(tgtKey)) {
                        commonBusinessKeys.add(srcKey.toLowerCase());
                    }
                }
            }

            if (!commonBusinessKeys.isEmpty()) {
                keyColumns.addAll(commonBusinessKeys);
                logger.info("Using business keys for table mapping: {}", keyColumns);
            }
        }

        // Last resort: use all common columns
        if (keyColumns.isEmpty()) {
            keyColumns.addAll(srcMetadata.getAllColumns().stream()
                    .filter(col -> tgtMetadata.getAllColumns().stream()
                            .anyMatch(tgtCol -> tgtCol.equalsIgnoreCase(col)))
                    .map(String::toLowerCase)
                    .collect(Collectors.toList()));
            logger.info("Using all common columns as keys for table mapping: {}", keyColumns);
        }

        mapping.setKeyColumns(keyColumns);
        return mapping;
    }

    /**
     * Main method to compare databases and generate Excel report
     */
    public File compareAndGenerateReport(
            String srcDbType, String tgtDbType,
            String srcHost, int srcPort, String srcDbName, String srcUsername, String srcPassword,
            String tgtHost, int tgtPort, String tgtDbName, String tgtUsername, String tgtPassword,
            String outputPath, List<TableMapping> selectedTables) {
        try {
            logger.info("Starting database comparison...");
            List<TableMapping> tableMappings = new ArrayList<>();
            if (selectedTables != null && !selectedTables.isEmpty()) {
                // Filter tables based on the provided mappings
                tableMappings.addAll(selectedTables);

                // Ensure all table mappings have database connection details
                for (TableMapping mapping : tableMappings) {
                    if (mapping.getSourceDbType() == null) {
                        mapping.setSourceDbType(srcDbType);
                        mapping.setSourceHost(srcHost);
                        mapping.setSourcePort(srcPort);
                        mapping.setSourceDbName(srcDbName);
                        mapping.setSourceUsername(srcUsername);
                        mapping.setSourcePassword(srcPassword);

                        mapping.setTargetDbType(tgtDbType);
                        mapping.setTargetHost(tgtHost);
                        mapping.setTargetPort(tgtPort);
                        mapping.setTargetDbName(tgtDbName);
                        mapping.setTargetUsername(tgtUsername);
                        mapping.setTargetPassword(tgtPassword);
                    }
                }
            } else {
                // Find all common tables if no mappings are provided
                tableMappings = findCommonTables(
                        srcDbType, tgtDbType,
                        srcHost, srcPort, srcDbName, srcUsername, srcPassword,
                        tgtHost, tgtPort, tgtDbName, tgtUsername, tgtPassword);
            }
            if (tableMappings.isEmpty()) {
                logger.warn("No tables found for comparison.");
                return null;
            }
            Map<String, Object> allResults = compareTables(tableMappings);
            // Generate Excel report
            File excelFile = excelGenerator.generateExcelReport(allResults, outputPath, tableMappings);
            logger.info("Excel report generated at: {}", excelFile.getAbsolutePath());
            return excelFile;
        } catch (Exception e) {
            logger.error("Error comparing databases: ", e);
            throw new RuntimeException("Database comparison failed: " + e.getMessage(), e);
        }
    }

    /**
     * Compare tables between databases
     */
    private Map<String, Object> compareTables(List<TableMapping> tableMappings) {
        Map<String, Object> allResults = new HashMap<>();
        List<Map<String, Object>> allDifferences = new ArrayList<>();
        List<Map<String, Object>> allUnmatchedSource = new ArrayList<>();
        List<Map<String, Object>> allUnmatchedTarget = new ArrayList<>();
        int totalExactMatches = 0;

        for (TableMapping mapping : tableMappings) {
            try {
                logger.info("Starting comparison for table mapping: {}", mapping);

                // Identify primary keys if not already set
                if (mapping.getKeyColumns() == null || mapping.getKeyColumns().isEmpty()) {
                    List<String> primaryKeys = identifyPrimaryKeys(
                        mapping.getSourceDbType(), mapping.getSourceHost(),
                        mapping.getSourcePort(), mapping.getSourceDbName(),
                        mapping.getSourceUsername(), mapping.getSourcePassword(),
                        mapping.getSourceTable()
                    );

                    if (!primaryKeys.isEmpty()) {
                        mapping.setKeyColumns(primaryKeys);
                        logger.info("Using identified primary keys for comparison: {}", primaryKeys);
                    } else {
                        logger.warn("No primary keys identified for table {}. Attempting to use business keys.",
                            mapping.getSourceTable());
                        List<String> businessKeys = identifyBusinessKeys(
                            mapping.getSourceDbType(), mapping.getSourceHost(),
                            mapping.getSourcePort(), mapping.getSourceDbName(),
                            mapping.getSourceUsername(), mapping.getSourcePassword(),
                            mapping.getSourceTable()
                        );

                        if (!businessKeys.isEmpty()) {
                            mapping.setKeyColumns(businessKeys);
                            logger.info("Using identified business keys for comparison: {}", businessKeys);
                        } else {
                            logger.error("No keys found for table {}. Comparison may be inaccurate.",
                                mapping.getSourceTable());
                        }
                    }
                }

                // Get data from both databases
                List<Map<String, Object>> sourceData = getTableData(
                    mapping.getSourceDbType(), mapping.getSourceHost(),
                    mapping.getSourcePort(), mapping.getSourceDbName(),
                    mapping.getSourceUsername(), mapping.getSourcePassword(),
                    mapping.getSourceTable()
                );

                List<Map<String, Object>> targetData = getTableData(
                    mapping.getTargetDbType(), mapping.getTargetHost(),
                    mapping.getTargetPort(), mapping.getTargetDbName(),
                    mapping.getTargetUsername(), mapping.getTargetPassword(),
                    mapping.getTargetTable()
                );

                logger.info("Retrieved {} records from source and {} records from target for table {}",
                    sourceData.size(), targetData.size(), mapping.getSourceTable());

                // Compare the data using the identified keys
                Map<String, Object> comparisonResult = compareTableData(sourceData, targetData, mapping);

                // Aggregate results
                allDifferences.addAll((List<Map<String, Object>>) comparisonResult.get("differences"));
                allUnmatchedSource.addAll((List<Map<String, Object>>) comparisonResult.get("unmatched_source"));
                allUnmatchedTarget.addAll((List<Map<String, Object>>) comparisonResult.get("unmatched_target"));
                totalExactMatches += (Integer) comparisonResult.get("exact_matches");

            } catch (Exception e) {
                logger.error("Error comparing table {}: {}", mapping.getSourceTable(), e.getMessage());
            }
        }

        allResults.put("differences", allDifferences);
        allResults.put("unmatched_source", allUnmatchedSource);
        allResults.put("unmatched_target", allUnmatchedTarget);
        allResults.put("exact_matches", totalExactMatches);

        return allResults;
    }

    /**
     * Compares data between source and target tables using identified keys
     */
    private Map<String, Object> compareTableData(List<Map<String, Object>> sourceData,
                                               List<Map<String, Object>> targetData,
                                               TableMapping mapping) {
        Map<String, Object> results = new HashMap<>();
        List<Map<String, Object>> differences = new ArrayList<>();
        List<Map<String, Object>> unmatchedSource = new ArrayList<>();
        List<Map<String, Object>> unmatchedTarget = new ArrayList<>();
        int exactMatches = 0;

        // Create maps for faster lookup using composite keys
        Map<String, Map<String, Object>> sourceMap = new HashMap<>();
        Map<String, Map<String, Object>> targetMap = new HashMap<>();

        logger.info("Using key columns for comparison: {}", mapping.getKeyColumns());

        // Build composite keys from all key columns
        for (Map<String, Object> record : sourceData) {
            String compositeKey = buildCompositeKey(record, mapping.getKeyColumns());
            if (compositeKey != null) {
                sourceMap.put(compositeKey, record);
                logger.debug("Source record key: {}", compositeKey);
            } else {
                logger.warn("Could not build composite key for source record: {}", record);
            }
        }

        for (Map<String, Object> record : targetData) {
            String compositeKey = buildCompositeKey(record, mapping.getKeyColumns());
            if (compositeKey != null) {
                targetMap.put(compositeKey, record);
                logger.debug("Target record key: {}", compositeKey);
            } else {
                logger.warn("Could not build composite key for target record: {}", record);
            }
        }

        // Compare records
        for (Map.Entry<String, Map<String, Object>> entry : sourceMap.entrySet()) {
            String key = entry.getKey();
            Map<String, Object> sourceRecord = entry.getValue();
            Map<String, Object> targetRecord = targetMap.get(key);

            if (targetRecord != null) {
                // Compare fields
                Map<String, Object> comparison = compareRecordFields(sourceRecord, targetRecord);
                List<Map<String, String>> fieldDifferences = (List<Map<String, String>>) comparison.get("differences");

                if (!fieldDifferences.isEmpty()) {
                    Map<String, Object> diffRecord = new HashMap<>();
                    diffRecord.put("table", mapping.getSourceTable());
                    diffRecord.put("key", key);
                    diffRecord.put("source_record", sourceRecord);
                    diffRecord.put("target_record", targetRecord);
                    diffRecord.put("differences", fieldDifferences);
                    differences.add(diffRecord);
                    logger.debug("Found differences for key {}: {}", key, fieldDifferences);
                } else {
                    exactMatches++;
                    logger.debug("Exact match found for key: {}", key);
                }
                targetMap.remove(key);
            } else {
                // Record exists in source but not in target
                unmatchedSource.add(sourceRecord);
                logger.debug("Record with key {} exists in source but not in target", key);
            }
        }

        // Remaining records in target are unmatched
        unmatchedTarget.addAll(targetMap.values());
        if (!targetMap.isEmpty()) {
            logger.debug("Found {} records in target that don't exist in source", targetMap.size());
        }

        results.put("differences", differences);
        results.put("unmatched_source", unmatchedSource);
        results.put("unmatched_target", unmatchedTarget);
        results.put("exact_matches", exactMatches);

        logger.info("Comparison results for table {}: {} differences, {} unmatched in source, {} unmatched in target, {} exact matches",
            mapping.getSourceTable(), differences.size(), unmatchedSource.size(), unmatchedTarget.size(), exactMatches);

        return results;
    }

    /**
     * Compares individual fields between two records
     */
    private Map<String, Object> compareRecordFields(Map<String, Object> sourceRecord, Map<String, Object> targetRecord) {
        Map<String, Object> result = new HashMap<>();
        List<Map<String, String>> differences = new ArrayList<>();

        // Compare all fields in source record
        for (Map.Entry<String, Object> entry : sourceRecord.entrySet()) {
            String field = entry.getKey();
            Object sourceValue = normalizeValue(entry.getValue()); // Normalize source value
            Object targetValue = normalizeValue(targetRecord.get(field)); // Normalize target value

            if (!Objects.equals(sourceValue, targetValue)) {
                Map<String, String> difference = new HashMap<>();
                difference.put("field", field);
                difference.put("source", sourceValue != null ? sourceValue.toString() : "<NULL>");
                difference.put("target", targetValue != null ? targetValue.toString() : "<NULL>");
                differences.add(difference);
            }
        }

        // Check for fields only in target
        for (String field : targetRecord.keySet()) {
            if (!sourceRecord.containsKey(field)) {
                Object targetValue = normalizeValue(targetRecord.get(field)); // Normalize target value
                Map<String, String> difference = new HashMap<>();
                difference.put("field", field);
                difference.put("source", "<NULL>");
                difference.put("target", targetValue != null ? targetValue.toString() : "<NULL>");
                differences.add(difference);
            }
        }

        result.put("differences", differences);
        return result;
    }

    /*Normalize values to ensure consistent comparison across databases*/
    private Object normalizeValue(Object value) {
        return NormalizationUtils.normalizeValue(value);
    }

    /**
     * Builds a composite key from multiple columns
     */
    private String buildCompositeKey(Map<String, Object> record, List<String> keyColumns) {
        if (keyColumns == null || keyColumns.isEmpty() || record == null) {
            return null;
        }
        StringBuilder key = new StringBuilder();
        for (String column : keyColumns) {
            Object value = record.get(column);
            if (value != null) {
                if (key.length() > 0) {
                    key.append(":");
                }
                // Normalize the value before building the composite key
                value = normalizeValue(value);
                key.append(value.toString());
            }
        }
        return key.length() > 0 ? key.toString() : null;
    }


    /**
     * Get JDBC URL dynamically from configuration
     */
    private String getJdbcUrl(String dbType, String host, int port, String dbName) {
        logger.debug("Getting JDBC URL for dbType: {}, host: {}, port: {}, dbName: {}", dbType, host, port, dbName);

        if (dbType == null || dbType.trim().isEmpty()) {
            logger.error("Database type is null or empty");
            throw new IllegalArgumentException("Database type cannot be null or empty");
        }

        if (host == null || host.trim().isEmpty()) {
            logger.error("Host is null or empty");
            throw new IllegalArgumentException("Host cannot be null or empty");
        }

        if (dbName == null || dbName.trim().isEmpty()) {
            logger.error("Database name is null or empty");
            throw new IllegalArgumentException("Database name cannot be null or empty");
        }

        if (databaseConfig == null) {
            logger.error("DatabaseConfig is null");
            throw new IllegalStateException("DatabaseConfig is null. Check your Spring configuration.");
        }

        if (databaseConfig.getJdbcUrl() == null) {
            logger.error("databaseConfig.getJdbcUrl() is null");
            throw new IllegalStateException("JDBC URL configuration is null. Check your application.properties file.");
        }

        if (databaseConfig.getJdbcUrl().isEmpty()) {
            logger.error("databaseConfig.getJdbcUrl() is empty");
            throw new IllegalStateException("JDBC URL configuration is empty. Check your application.properties file.");
        }

        logger.debug("Available JDBC URL keys: {}", databaseConfig.getJdbcUrl().keySet());

        String dbTypeLower = dbType.toLowerCase();
        logger.debug("Looking for JDBC URL with key: {}", dbTypeLower);

        String baseJdbcUrl = databaseConfig.getJdbcUrl().get(dbTypeLower);

        if (baseJdbcUrl == null) {
            // Try with original case
            logger.debug("Not found with lowercase, trying with original case: {}", dbType);
            baseJdbcUrl = databaseConfig.getJdbcUrl().get(dbType);

            if (baseJdbcUrl == null) {
                logger.error("No JDBC URL found for database type: {}", dbType);
                throw new IllegalArgumentException("Unsupported database type: " + dbType +
                    ". Available types: " + String.join(", ", databaseConfig.getJdbcUrl().keySet()));
            }
        }

        logger.info("Using JDBC URL for {}: {}", dbType, baseJdbcUrl);
        return baseJdbcUrl + host + ":" + port + "/" + dbName;
    }


    /**
     * Get a database connection using JDBC URL
     */
    private Connection getConnection(String jdbcUrl, String username, String password) throws SQLException {
        try {
            // Load the driver class dynamically based on the JDBC URL
            String driverClass = null;
            
            // Extract database type from JDBC URL
            String dbType = null;
            if (jdbcUrl.contains("mysql")) {
                dbType = "mysql";
            } else if (jdbcUrl.contains("postgresql")) {
                dbType = "postgresql";
            } else if (jdbcUrl.contains("sqlserver")) {
                dbType = "sqlserver";
            } else if (jdbcUrl.contains("oracle")) {
                dbType = "oracle";
            }
            
            // Get driver class from configuration
            if (dbType != null && databaseConfig != null && databaseConfig.getDriver() != null) {
                driverClass = databaseConfig.getDriver().get(dbType);
            }
            
            // Fallback to hardcoded values if configuration is not available
            if (driverClass == null) {
                if (jdbcUrl.contains("mysql")) {
                    driverClass = "com.mysql.cj.jdbc.Driver";
                } else if (jdbcUrl.contains("postgresql")) {
                    driverClass = "org.postgresql.Driver";
                } else if (jdbcUrl.contains("sqlserver")) {
                    driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
                } else if (jdbcUrl.contains("oracle")) {
                    driverClass = "oracle.jdbc.OracleDriver";
                }
            }

            if (driverClass != null) {
                Class.forName(driverClass);
            }

            return DriverManager.getConnection(jdbcUrl, username, password);
        } catch (ClassNotFoundException e) {
            logger.error("Database driver not found: {}", e.getMessage());
            throw new SQLException("Database driver not found", e);
        }
    }

    /**
     * Get a database connection using individual connection parameters
     */
    private Connection getConnection(String dbType, String host, int port, String dbName,
                                   String username, String password) throws SQLException {
        String jdbcUrl = getJdbcUrl(dbType, host, port, dbName);
        return getConnection(jdbcUrl, username, password);
    }

    /**
     * Close a database connection safely
     */
    private void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
                logger.debug("Database connection closed successfully");
            } catch (SQLException e) {
                logger.warn("Error closing database connection: {}", e.getMessage());
            }
        }
    }

    /**
     * Get table data as a list of maps
     */
    private List<Map<String, Object>> getTableData(String dbType, String host, int port, String dbName, String username, String password, String tableName) {
        Connection conn = null;
        try {
            conn = getConnection(dbType, host, port, dbName, username, password);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            List<Map<String, Object>> data = new ArrayList<>();

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i).toLowerCase();
                    Object value = rs.getObject(i);
                    row.put(columnName, value);
                }
                data.add(row);
            }

            return data;
        } catch (SQLException e) {
            logger.error("Error fetching data from table {}: {}", tableName, e.getMessage());
            throw new RuntimeException("Failed to fetch table data", e);
        } finally {
            closeConnection(conn);
        }
    }
}