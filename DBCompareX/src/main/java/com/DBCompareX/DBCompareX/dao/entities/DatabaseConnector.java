package com.DBCompareX.DBCompareX.dao.entities;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class DatabaseConnector {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseConnector.class);
    private final Map<String, HikariDataSource> dataSources = new ConcurrentHashMap<>();

    /**
     * Creates and caches a HikariCP DataSource for the given database.
     * If the connection already exists, it returns the cached one.
     */
    public DataSource getDataSource(String dbType, String host, int port, String dbName, String username, String password) {
        String key = generateKey(dbType, host, port, dbName);
        return dataSources.computeIfAbsent(key, k -> createDataSource(dbType, host, port, dbName, username, password));
    }

    /**
     * Establishes connections to both source and target databases.
     * @return true if both connections are successful, false otherwise.
     */
    public boolean connectToDatabases(String srcDbType, String srcHost, int srcPort, String srcDbName, String srcUser, String srcPass,
                                      String tgtDbType, String tgtHost, int tgtPort, String tgtDbName, String tgtUser, String tgtPass) {
        try {
            DataSource sourceDS = getDataSource(srcDbType, srcHost, srcPort, srcDbName, srcUser, srcPass);
            DataSource targetDS = getDataSource(tgtDbType, tgtHost, tgtPort, tgtDbName, tgtUser, tgtPass);

            boolean isConnected = testConnection(sourceDS) && testConnection(targetDS);
            logger.info("Database connection status: {}", isConnected ? "Connected" : "Failed to connect");
            return isConnected;
        } catch (Exception e) {
            logger.error("Failed to connect to databases: {}", e.getMessage());
            closeAllConnections(); // Clear cached connections on failure
            return false;
        }
    }

    private boolean testConnection(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection()) {
            return connection != null && !connection.isClosed();
        } catch (Exception e) {
            logger.error("Connection test failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Generates a unique key for caching connections.
     */
    private String generateKey(String dbType, String host, int port, String dbName) {
        return dbType + "_" + host + "_" + port + "_" + dbName;
    }

    /**
     * Creates a new HikariCP DataSource.
     */
    private HikariDataSource createDataSource(String dbType, String host, int port, String dbName, String username, String password) {
        String jdbcUrl = getJdbcUrl(dbType, host, port, dbName);
        if (jdbcUrl == null) {
            logger.error("Unsupported database type: {}", dbType);
            return null;
        }

        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(jdbcUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setMaximumPoolSize(10);
        dataSource.setMinimumIdle(2);
        dataSource.setIdleTimeout(30000);
        dataSource.setMaxLifetime(180000);
        dataSource.setConnectionTimeout(30000);

        logger.info("Created DataSource for {} at {}:{}, JDBC URL: {}", dbType, host, port, jdbcUrl);
        return dataSource;
    }

    /**
     * Returns the JDBC URL for different database types.
     */
    private String getJdbcUrl(String dbType, String host, int port, String dbName) {
        return switch (dbType.toLowerCase()) {
            case "mysql" -> "jdbc:mysql://" + host + ":" + port + "/" + dbName + "?serverTimezone=UTC";
            case "postgresql" -> "jdbc:postgresql://" + host + ":" + port + "/" + dbName;
            case "sqlserver" -> "jdbc:sqlserver://" + host + ":" + port + ";databaseName=" + dbName;
            case "oracle" -> "jdbc:oracle:thin:@//" + host + ":" + port + "/" + dbName; // Use service name with double slashes
            default -> null;
        };
    }

    /**
     * Closes all database connections.
     */
    public void closeAllConnections() {
        dataSources.values().forEach(HikariDataSource::close);
        dataSources.clear();
        logger.info("All database connections closed.");
    }
}
