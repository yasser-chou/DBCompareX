package com.DBCompareX.DBCompareX.service;

import com.DBCompareX.DBCompareX.dao.entities.DatabaseConnector;
import com.DBCompareX.DBCompareX.dao.entities.ExcelGenerator;
import com.DBCompareX.DBCompareX.dao.entities.TableMapping;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.shaded.org.apache.commons.lang3.ArrayUtils.toArray;

@Service
public class TableSchemaExtractor {
    private static final Logger logger = LoggerFactory.getLogger(TableSchemaExtractor.class);
    private final DatabaseConnector databaseConnector;
    private final SparkSession sparkSession;

    @Autowired
    private ExcelGenerator excelGenerator;

    public TableSchemaExtractor(DatabaseConnector databaseConnector, SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        this.databaseConnector = databaseConnector;
    }

    public Map<String, Object> fetchDataFromBothDatabases(
            String srcDbType, String tgtDbType,
            String srcHost, int srcPort, String srcDbName, String srcUsername, String srcPassword,
            String tgtHost, int tgtPort, String tgtDbName, String tgtUsername, String tgtPassword,
            List<TableMapping> tableMappings) {
        try {
            logger.info("Starting data comparison for tables: {}", tableMappings);
            Dataset<Row> srcDf = loadDatabaseData(srcDbType, srcHost, srcPort, srcDbName, srcUsername, srcPassword, tableMappings, true);
            Dataset<Row> tgtDf = loadDatabaseData(tgtDbType, tgtHost, tgtPort, tgtDbName, tgtUsername, tgtPassword, tableMappings, false);
            return compareDataframes(srcDf, tgtDf);
        } catch (Exception e) {
            logger.error("Error comparing databases: ", e);
            throw new RuntimeException("Failed to compare databases: " + e.getMessage(), e);
        }
    }

    /**
     * Generates an Excel report from the comparison results.
     */
    public String generateExcelReport(Map<String, Object> comparisonResults, String outputPath) {
        try {
            if (comparisonResults == null) {
                throw new IllegalArgumentException("Comparison results cannot be null");
            }

            Map<String, List<Map<String, Object>>> formattedResults = new HashMap<>();

            // Process each section dynamically
            String[] sections = {"differences", "unmatched_source", "unmatched_target", "identical"};
            for (String section : sections) {
                List<Map<String, Object>> processedData = new ArrayList<>();
                Object sectionData = comparisonResults.get(section);

                if (sectionData instanceof List) {
                    List<String> jsonList = (List<String>) sectionData;
                    for (String json : jsonList) {
                        if (json != null) {
                            Map<String, Object> parsedData = parseJsonToMap(json);
                            if (parsedData != null && !parsedData.isEmpty()) {
                                // Sanitize data to ensure no null values
                                Map<String, Object> sanitizedData = parsedData.entrySet().stream()
                                        .collect(Collectors.toMap(
                                                Map.Entry::getKey,
                                                entry -> entry.getValue() != null ? entry.getValue() : ""
                                        ));
                                processedData.add(sanitizedData);
                            }
                        }
                    }
                }
                formattedResults.put(section, processedData);
            }

            // Generate the Excel report
            excelGenerator.generateComparisonReport(formattedResults, outputPath);
            return outputPath;
        } catch (Exception e) {
            logger.error("Error generating Excel report: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to generate Excel report: " + e.getMessage(), e);
        }
    }

    private void processSection(Map<String, Object> comparisonResults, String sectionName, Map<String, List<Map<String, Object>>> formattedResults) {
        List<Map<String, Object>> sectionData = new ArrayList<>();
        List<String> jsonList = (List<String>) comparisonResults.get(sectionName);
        if (jsonList != null) {
            for (String json : jsonList) {
                Map<String, Object> parsedData = parseJsonToMap(json);
                if (parsedData != null) {
                    sectionData.add(parsedData);
                }
            }
        }
        formattedResults.put(sectionName, sectionData);
    }
    /**
     * Helper method to parse JSON string to Map.
     */
    private Map<String, Object> parseJsonToMap(String json) {
        if (json == null || json.trim().isEmpty()) {
            return new HashMap<>();
        }

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            Map<String, Object> result = objectMapper.readValue(
                    json,
                    new TypeReference<Map<String, Object>>() {}
            );
            return result != null ? result : new HashMap<>();
        } catch (JsonProcessingException e) {
            logger.error("Error parsing JSON: {}", e.getMessage(), e);
            return new HashMap<>();
        }
    }

    /**
     * Loads data from the database into a Spark DataFrame.
     */
    private Dataset<Row> loadDatabaseData(String dbType, String host, int port, String dbName, String username, String password, List<TableMapping> tableMappings, boolean isSource) {
        try {
            String jdbcUrl = getJdbcUrl(dbType, host, port, dbName);
            Properties connectionProperties = new Properties();
            connectionProperties.put("user", username);
            connectionProperties.put("password", password);
            connectionProperties.put("driver", getDriverClass(dbType));

            Dataset<Row> combinedDf = null;
            for (TableMapping mapping : tableMappings) {
                String tableName = isSource ? mapping.getSourceTable() : mapping.getTargetTable();
                logger.info("Loading data from table: {}", tableName);
                Dataset<Row> tableDf = sparkSession.read()
                        .jdbc(jdbcUrl, tableName, connectionProperties)
                        .withColumn("source_table", functions.lit(mapping.getSourceTable()))
                        .withColumn("target_table", functions.lit(mapping.getTargetTable()));
                tableDf = normalizeColumnNames(tableDf);
                combinedDf = (combinedDf == null) ? tableDf : combinedDf.unionByName(tableDf, true);
            }
            return combinedDf;
        } catch (Exception e) {
            logger.error("Error loading data from {}:{}@{}/{}: {}", username, password, host, dbName, e.getMessage());
            throw new RuntimeException("Failed to load data: " + e.getMessage(), e);
        }
    }

    /**
     * Normalizes column names to lowercase.
     */
    private Dataset<Row> normalizeColumnNames(Dataset<Row> df) {
        return df.toDF(Arrays.stream(df.columns())
                .map(col -> col.toLowerCase().replaceAll("\\s+", "_")) // Replace spaces with underscores
                .toArray(String[]::new));
    }

    private Map<String, Object> normalizeRow(Map<String, Object> row) {
        return row.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue() != null ? entry.getValue() : "N/A"
                ));
    }


    /**
     * Compares two DataFrames and identifies differences, unmatched records, and identical records.
     */
    private Map<String, Object> compareDataframes(Dataset<Row> srcDf, Dataset<Row> tgtDf) {
        // Register UDFs only once
        registerUDFs();

        // Convert date/timestamp columns to formatted strings
        Dataset<Row> formattedSrcDf = formatDateColumns(srcDf);
        Dataset<Row> formattedTgtDf = formatDateColumns(tgtDf);

        // Get common columns first
        List<String> commonColumns = Arrays.stream(formattedSrcDf.columns())
                .filter(col -> Arrays.asList(formattedTgtDf.columns()).contains(col))
                .collect(Collectors.toList());;
        logger.info("Common Columns: {}", commonColumns);

        // Determine comparison keys
        List<ColumnMatch> keyColumns = determineComparisonKeys(formattedSrcDf, formattedTgtDf);
        if (keyColumns.isEmpty()) {
            logger.warn("No comparison keys found. Using all common columns.");
            keyColumns = commonColumns.stream()
                    .map(col -> new ColumnMatch(col, 0.8, false, false))
                    .collect(Collectors.toList());
            logger.warn("No unique comparison keys found. Using all common columns as fallback keys: {}", commonColumns);

        }

        // Create join condition and perform join
        Column joinCondition = createJoinCondition(keyColumns);
        Dataset<Row> joinedDf = formattedSrcDf.as("src")
                .join(formattedTgtDf.as("tgt"), joinCondition, "full_outer");

        // Calculate matches and differences
        Dataset<Row> exactMatches = findExactMatches(joinedDf, commonColumns.toArray(new String[0]));
        Dataset<Row> unmatchedSource = findUnmatchedRecords(joinedDf, "src", "tgt");
        Dataset<Row> unmatchedTarget = findUnmatchedRecords(joinedDf, "tgt", "src");
        Dataset<Row> differences = calculateDifferences(joinedDf, exactMatches, commonColumns);

        // Convert results to JSON strings with proper handling
        Map<String, Object> results = new HashMap<>();
        results.put("identical", convertToJsonList(exactMatches));
        results.put("unmatched_source", convertToJsonList(unmatchedSource));
        results.put("unmatched_target", convertToJsonList(unmatchedTarget));
        results.put("differences", convertToJsonList(differences));

        return results;
    }

    private void registerUDFs() {
        // DateTime formatter UDF
        sparkSession.udf().register("formatDateTime", (Object value) -> {
            if (value == null) {
                return null;
            }
            try {
                if (value instanceof java.sql.Date) {
                    return new SimpleDateFormat("yyyy-MM-dd").format(value);
                }
                if (value instanceof java.sql.Timestamp) {
                    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(value);
                }
                return String.valueOf(value);
            } catch (Exception e) {
                logger.warn("Error formatting datetime value: {}", value, e);
                return String.valueOf(value);
            }
        }, DataTypes.StringType);

        // Levenshtein UDF
        sparkSession.udf().register("levenshtein",
                (String s1, String s2) -> {
                    if (s1 == null || s2 == null) {
                        return null;
                    }
                    return org.apache.commons.text.similarity.LevenshteinDistance
                            .getDefaultInstance()
                            .apply(s1, s2);
                },
                DataTypes.IntegerType);

        // Hash value UDF
        sparkSession.udf().register("hashValue",
                (String value) -> value != null ? Integer.toHexString(value.hashCode()) : null,
                DataTypes.StringType);
    }

    private Dataset<Row> formatDateColumns(Dataset<Row> df) {
        Dataset<Row> result = df;
        for (String column : df.columns()) {
            try {
                if (df.schema().apply(column).dataType() instanceof org.apache.spark.sql.types.DateType
                        || df.schema().apply(column).dataType() instanceof org.apache.spark.sql.types.TimestampType) {
                    result = result.withColumn(column,
                            functions.callUDF("formatDateTime", result.col(column)));
                }
            } catch (Exception e) {
                logger.warn("Error formatting column {}: {}", column, e.getMessage());
            }
        }
        return result;
    }

    private List<String> convertToJsonList(Dataset<Row> df) {
        try {
            return df.select(functions.to_json(functions.struct("*")).as("data"))
                    .collectAsList()
                    .stream()
                    .map(row -> row.getString(0))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            logger.error("Error converting dataset to JSON list: {}", e.getMessage(), e);
            return new ArrayList<>();
        }
    }

    /**
     * Represents a column match with metadata.
     */
    private static class ColumnMatch {
        String name;
        double similarityThreshold;
        boolean isPrimaryKey;
        boolean isUnique;

        ColumnMatch(String name, double similarityThreshold, boolean isPrimaryKey, boolean isUnique) {
            this.name = name;
            this.similarityThreshold = similarityThreshold;
            this.isPrimaryKey = isPrimaryKey;
            this.isUnique = isUnique;
        }

        public boolean isUnique() {
            return isUnique;
        }
    }

    /**
     * Determines comparison keys based on column patterns and uniqueness.
     */
    private List<ColumnMatch> determineComparisonKeys(Dataset<Row> srcDf, Dataset<Row> tgtDf) {
        List<ColumnMatch> keyColumns = new ArrayList<>();
        Set<String> keyPatterns = Set.of("id", "code", "email", "phone", "transaction_id");

        List<String> commonColumns = Arrays.stream(srcDf.columns())
                .filter(col -> Arrays.asList(tgtDf.columns()).contains(col))
                .filter(col -> !isTechnicalColumn(col))
                .collect(Collectors.toList());

        for (String col : commonColumns) {
            String colLower = col.toLowerCase();
            if (keyPatterns.stream().anyMatch(colLower::contains)) {
                double threshold = colLower.contains("id") ? 1.0 : 0.9;
                boolean isUnique = isColumnUnique(srcDf, col);
                keyColumns.add(new ColumnMatch(col, threshold, false, isUnique));
            }
        }

        // Fallback: Use composite keys if no unique keys are found
        if (keyColumns.isEmpty()) {
            keyColumns = commonColumns.stream()
                    .map(col -> new ColumnMatch(col, 0.8, false, false))
                    .collect(Collectors.toList());
            logger.warn("No unique comparison keys found. Using all common columns as fallback keys: {}", commonColumns);
        }

        return keyColumns;
    }

    /**
     * Checks if a column is a technical column (e.g., id, created_at).
     */
    private boolean isTechnicalColumn(String columnName) {
        Set<String> technicalColumns = Set.of("id", "created_at", "updated_at", "modified_at", "timestamp", "uuid");
        return technicalColumns.stream().anyMatch(techCol -> columnName.toLowerCase().contains(techCol));
    }

    /**
     * Checks if a column is unique in the dataset.
     */
    private boolean isColumnUnique(Dataset<Row> df, String column) {
        long totalCount = df.count();
        long distinctCount = df.select(column).distinct().count();
        return distinctCount >= totalCount * 0.9;
    }

    /**
     * Creates a join condition based on comparison keys.
     */
    private Column createJoinCondition(List<ColumnMatch> keyColumns) {
        if (keyColumns.isEmpty()) {
            return functions.lit(true);  // Fallback to cartesian join if no keys found
        }
        return keyColumns.stream()
                .map(key -> {
                    if (isNumericColumn(key.name)) {
                        return createFuzzyNumericMatchCondition(key.name, key.similarityThreshold);
                    } else if (key.name.equalsIgnoreCase("card_number")) {
                        return functions.col("src." + key.name).equalTo(functions.col("tgt." + key.name));
                    } else {
                        return createHashMatchCondition(key.name);
                    }
                })
                .reduce(Column::and)
                .orElse(functions.lit(true));
    }

    /**
     * Checks if a column is numeric.
     */
    private boolean isNumericColumn(String columnName) {
        return columnName.toLowerCase().contains("amount") || columnName.toLowerCase().contains("price");
    }

    /**
     * Creates a fuzzy match condition for numeric columns.
     */
    private Column createFuzzyNumericMatchCondition(String srcCol, double threshold) {
        return functions.abs(
                functions.col("src." + srcCol).minus(functions.col("tgt." + srcCol))
        ).divide(
                functions.greatest(
                        functions.abs(functions.col("src." + srcCol)),
                        functions.abs(functions.col("tgt." + srcCol))
                )
        ).lt(threshold);
    }

    /**
     * Creates a hash-based match condition for important fields.
     */
    private Column createHashMatchCondition(String srcCol) {
        return functions.callUDF("hashValue", functions.col("src." + srcCol))
                .equalTo(functions.callUDF("hashValue", functions.col("tgt." + srcCol)));
    }

    /**
     * Converts all column values to lowercase for case-insensitive comparison.
     */
    private Dataset<Row> convertToLowercase(Dataset<Row> df) {
        return df.select(Arrays.stream(df.columns())
                .map(col -> functions.lower(functions.col(col)).as(col))
                .toArray(Column[]::new));
    }

    /**
     * Finds exact matches between source and target DataFrames.
     */
    private Dataset<Row> findExactMatches(Dataset<Row> joinedDf, String[] columns) {
        // Create exact match condition for all columns
        Column matchCondition = Arrays.stream(columns)
                .map(col -> functions.col("src." + col).equalTo(functions.col("tgt." + col))
                        .or(functions.col("src." + col).isNull()
                                .and(functions.col("tgt." + col).isNull())))
                .reduce(Column::and)
                .orElse(functions.lit(true));

        // Add conditions to exclude nulls on both sides
        Column notAllNullCondition = Arrays.stream(columns)
                .map(col -> functions.col("src." + col).isNotNull()
                        .or(functions.col("tgt." + col).isNotNull()))
                .reduce(Column::or)
                .orElse(functions.lit(true));

        return joinedDf.filter(matchCondition.and(notAllNullCondition));
    }

    /**
     * Finds similar records based on fuzzy matching.
     */
    private Dataset<Row> findSimilarRecords(Dataset<Row> joinedDf, Dataset<Row> exactMatches, Column joinCondition) {
        return joinedDf.filter(joinCondition)
                .except(exactMatches);
    }

    /**
     * Finds unmatched records in one DataFrame compared to another.
     */
    private Dataset<Row> findUnmatchedRecords(Dataset<Row> joinedDf, String source, String target) {
        return joinedDf
                .filter(functions.col(target + "." + joinedDf.columns()[0]).isNull())
                .withColumn("reason", functions.lit("unmatched"))
                .select(functions.col(source + ".*"), functions.col("reason"));
    }

    /**
     * Calculates differences between source and target DataFrames.
     */
    private Dataset<Row> calculateDifferences(Dataset<Row> joinedDf, Dataset<Row> exactMatches, List<String> columns) {
        Dataset<Row> diffRecords = joinedDf.except(exactMatches);

        List<Column> diffColumns = new ArrayList<>();
        for (String col : columns) {
            diffColumns.add(functions.col("src." + col).as("src_" + col));
            diffColumns.add(functions.col("tgt." + col).as("tgt_" + col));
            diffColumns.add(functions.when(
                    functions.col("src." + col).isNull().or(functions.col("tgt." + col).isNull()),
                    "missing"
            ).when(
                    functions.col("src." + col).notEqual(functions.col("tgt." + col)),
                    "different"
            ).otherwise("match").as("status_" + col));
        }

        Column mismatchCount = columns.stream()
                .map(col -> functions.when(functions.col("status_" + col).notEqual("match"), 1).otherwise(0))
                .reduce(Column::plus)
                .orElse(functions.lit(0));

        Column potentialMatchFlag = mismatchCount.leq(2);
        Column manualReviewFlag = mismatchCount.gt(2).and(mismatchCount.leq(5));

        return diffRecords.select(Stream.concat(diffColumns.stream(), Stream.of(
                        mismatchCount.as("mismatch_count"),
                        potentialMatchFlag.as("potential_match"),
                        manualReviewFlag.as("manual_review")
                )).toArray(Column[]::new))
                .where(potentialMatchFlag.or(manualReviewFlag));
    }

    private String getJdbcUrl(String dbType, String host, int port, String dbName) {
        switch (dbType.toLowerCase()) {
            case "mysql":
                return String.format("jdbc:mysql://%s:%d/%s", host, port, dbName);
            case "postgresql":
                return String.format("jdbc:postgresql://%s:%d/%s", host, port, dbName);
            case "sqlserver":
                return String.format("jdbc:sqlserver://%s:%d;databaseName=%s", host, port, dbName);
            case "oracle":
                return String.format("jdbc:oracle:thin:@//%s:%d/%s", host, port, dbName);
            default:
                throw new IllegalArgumentException("Unsupported database type: " + dbType);
        }
    }

    private String getDriverClass(String dbType) {
        switch (dbType.toLowerCase()) {
            case "mysql":
                return "com.mysql.cj.jdbc.Driver";
            case "postgresql":
                return "org.postgresql.Driver";
            case "sqlserver":
                return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
            case "oracle":
                return "oracle.jdbc.driver.OracleDriver";
            default:
                throw new IllegalArgumentException("Unsupported database type: " + dbType);
        }
    }
}