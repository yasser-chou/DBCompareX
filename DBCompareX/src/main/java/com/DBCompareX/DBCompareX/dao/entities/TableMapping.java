package com.DBCompareX.DBCompareX.dao.entities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a mapping between source and target tables
 */
public class TableMapping {
    private static final Logger logger = LoggerFactory.getLogger(TableMapping.class);

    private String sourceTable;
    private String targetTable;
    private List<String> keyColumns;

    // Add database connection details
    private String sourceDbType;
    private String sourceHost;
    private int sourcePort;
    private String sourceDbName;
    private String sourceUsername;
    private String sourcePassword;

    private String targetDbType;
    private String targetHost;
    private int targetPort;
    private String targetDbName;
    private String targetUsername;
    private String targetPassword;

    public TableMapping() {
        this.keyColumns = new ArrayList<>();
    }

    public TableMapping(String sourceTable, String targetTable) {
        this.sourceTable = sourceTable;
        this.targetTable = targetTable;
        this.keyColumns = new ArrayList<>();
    }

    // Getters and setters
    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public List<String> getKeyColumns() {
        return keyColumns;
    }

    public void setKeyColumns(List<String> keyColumns) {
        logger.info("Setting key columns for table mapping {}: {}", sourceTable, keyColumns);
        this.keyColumns = keyColumns;
    }

    public void addKeyColumn(String column) {
        if (!this.keyColumns.contains(column)) {
            this.keyColumns.add(column);
            logger.info("Added key column {} to table mapping {}", column, sourceTable);
        }
    }

    public String getSourceDbType() {
        return sourceDbType;
    }

    public void setSourceDbType(String sourceDbType) {
        this.sourceDbType = sourceDbType;
    }

    public String getTargetDbType() {
        return targetDbType;
    }

    public void setTargetDbType(String targetDbType) {
        this.targetDbType = targetDbType;
    }

    public String getSourceHost() {
        return sourceHost;
    }

    public void setSourceHost(String sourceHost) {
        this.sourceHost = sourceHost;
    }

    public String getTargetHost() {
        return targetHost;
    }

    public void setTargetHost(String targetHost) {
        this.targetHost = targetHost;
    }

    public int getSourcePort() {
        return sourcePort;
    }

    public void setSourcePort(int sourcePort) {
        this.sourcePort = sourcePort;
    }

    public int getTargetPort() {
        return targetPort;
    }

    public void setTargetPort(int targetPort) {
        this.targetPort = targetPort;
    }

    public String getSourceDbName() {
        return sourceDbName;
    }

    public void setSourceDbName(String sourceDbName) {
        this.sourceDbName = sourceDbName;
    }

    public String getTargetDbName() {
        return targetDbName;
    }

    public void setTargetDbName(String targetDbName) {
        this.targetDbName = targetDbName;
    }

    public String getSourceUsername() {
        return sourceUsername;
    }

    public void setSourceUsername(String sourceUsername) {
        this.sourceUsername = sourceUsername;
    }

    public String getTargetUsername() {
        return targetUsername;
    }

    public void setTargetUsername(String targetUsername) {
        this.targetUsername = targetUsername;
    }

    public String getSourcePassword() {
        return sourcePassword;
    }

    public void setSourcePassword(String sourcePassword) {
        this.sourcePassword = sourcePassword;
    }

    public String getTargetPassword() {
        return targetPassword;
    }

    public void setTargetPassword(String targetPassword) {
        this.targetPassword = targetPassword;
    }

    @Override
    public String toString() {
        return "TableMapping{" +
                "sourceTable='" + sourceTable + '\'' +
                ", targetTable='" + targetTable + '\'' +
                ", keyColumns=" + keyColumns +
                ", sourceDbType='" + sourceDbType + '\'' +
                ", sourceHost='" + sourceHost + '\'' +
                ", sourcePort=" + sourcePort +
                ", sourceDbName='" + sourceDbName + '\'' +
                ", sourceUsername='" + sourceUsername + '\'' +
                ", sourcePassword='[PROTECTED]'" + // Avoid logging sensitive info
                ", targetDbType='" + targetDbType + '\'' +
                ", targetHost='" + targetHost + '\'' +
                ", targetPort=" + targetPort +
                ", targetDbName='" + targetDbName + '\'' +
                ", targetUsername='" + targetUsername + '\'' +
                ", targetPassword='[PROTECTED]'" + // Avoid logging sensitive info
                '}';
    }
}