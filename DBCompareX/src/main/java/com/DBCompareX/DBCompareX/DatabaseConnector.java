package com.DBCompareX.DBCompareX;

import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Component
public class DatabaseConnector {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseConnector.class);

    private Connection sourceConnection;
    private Connection targetConnection;

    public Connection connect(String dbType, String host, int port, String dbName, String username, String password) {
        String url = getJdbcUrl(dbType, host, port, dbName);
        if (url == null) {
            logger.error("Unsupported database type: {}", dbType);
            return null;
        }

        try {
            return DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            logger.error("Connection failed for {} at {}:{} -> {}", dbType, host, port, e.getMessage());
            return null;
        }
    }

    public boolean connectToDatabases(String srcDbType, String srcHost, int srcPort, String srcDbName, String srcUser, String srcPass,
                                      String tgtDbType, String tgtHost, int tgtPort, String tgtDbName, String tgtUser, String tgtPass) {
        sourceConnection = connect(srcDbType, srcHost, srcPort, srcDbName, srcUser, srcPass);
        targetConnection = connect(tgtDbType, tgtHost, tgtPort, tgtDbName, tgtUser, tgtPass);

        return sourceConnection != null && targetConnection != null;
    }

    private String getJdbcUrl(String dbType, String host, int port, String dbName) {
        return switch (dbType.toLowerCase()) {
            case "mysql" -> "jdbc:mysql://" + host + ":" + port + "/" + dbName + "?serverTimezone=UTC";
            case "postgresql" -> "jdbc:postgresql://" + host + ":" + port + "/" + dbName;
            case "sqlserver" -> "jdbc:sqlserver://" + host + ":" + port + ";databaseName=" + dbName;
            case "oracle" -> "jdbc:oracle:thin:@" + host + ":" + port + ":" + dbName;
            default -> null;
        };
    }

    public void closeConnections() {
        try {
            if (sourceConnection != null) {
                sourceConnection.close();
            }
            if (targetConnection != null) {
                targetConnection.close();
            }
        } catch (SQLException e) {
            logger.error("Error closing connection: {}", e.getMessage());
        }
    }

    public Connection getSourceConnection() {
        return sourceConnection;
    }

    public Connection getTargetConnection() {
        return targetConnection;
    }
}