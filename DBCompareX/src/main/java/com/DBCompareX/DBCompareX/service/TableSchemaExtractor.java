package com.DBCompareX.DBCompareX.service;


import com.DBCompareX.DBCompareX.dao.entities.DatabaseConnector;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class TableSchemaExtractor {

    private final DatabaseConnector databaseConnector;

    public TableSchemaExtractor(DatabaseConnector databaseConnector) {
        this.databaseConnector = databaseConnector;
    }

    //fetch the table names:

    public List<String> getAllTables(String dbType, String host, int port, String dbName, String username, String password) {
        List<String> tables = new ArrayList<>();

        try (Connection connection = databaseConnector.getDataSource(dbType, host, port, dbName, username, password).getConnection()) {
            String query;
            switch (dbType.toLowerCase()) {
                case "mysql":
                case "postgresql":
                    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = ?";
                    break;
                case "oracle":
                    query = "SELECT table_name FROM user_tables"; // No need for dbName parameter
                    break;
                case "sqlserver":
                    query = "SELECT table_name FROM information_schema.tables WHERE table_catalog = ?";
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported database type: " + dbType);
            }

            System.out.println("Executing query: " + query);

            try (PreparedStatement statement = connection.prepareStatement(query)) {
                if (!"oracle".equalsIgnoreCase(dbType)) { // Only set dbName for non-Oracle databases
                    statement.setString(1, dbName);
                }
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        tables.add(resultSet.getString("table_name"));
                    }
                }
            }
        } catch (SQLException e) {
            System.err.println("SQL Error: " + e.getMessage());
            if (e.getErrorCode() == 1017) { // ORA-01017: invalid username/password
                throw new RuntimeException("Invalid username or password: " + e.getMessage(), e);
            } else if (e.getErrorCode() == 12505) { // ORA-12505: listener error
                throw new RuntimeException("Listener error: SID or service name not registered. Ensure 'xepdb1' is correct.", e);
            } else {
                throw new RuntimeException("Error retrieving tables: " + e.getMessage(), e);
            }
        }

        System.out.println("Retrieved tables: " + tables);
        return tables;
    }



    //Get the table columns:
    public List<Map<String, Object>> getTableColumns(String dbType, String host, int port, String dbName, String username, String password, String tableName) {
        DataSource dataSource = databaseConnector.getDataSource(dbType, host, port, dbName, username, password);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        String query;
        switch (dbType.toLowerCase()) {
            case "mysql":
            case "postgresql":
                query = "SELECT column_name, data_type, is_nullable, character_maximum_length " +
                        "FROM information_schema.columns WHERE table_schema = ? AND table_name = ?";
                break;
            case "oracle":
                query = "SELECT column_name, data_type, nullable AS is_nullable, data_length AS character_maximum_length " +
                        "FROM user_tab_columns WHERE table_name = ?";
                break;
            case "sqlserver":
                query = "SELECT column_name, data_type, is_nullable, character_maximum_length " +
                        "FROM information_schema.columns WHERE table_catalog = ? AND table_name = ?";
                break;
            default:
                throw new IllegalArgumentException("Unsupported database type: " + dbType);
        }

        System.out.println("Executing query: " + query);

        if ("oracle".equalsIgnoreCase(dbType)) {
            return jdbcTemplate.query(query, new Object[]{tableName}, (rs, rowNum) -> {
                Map<String, Object> columnInfo = new HashMap<>();
                columnInfo.put("column_name", rs.getString("column_name"));
                columnInfo.put("data_type", rs.getString("data_type"));
                columnInfo.put("is_nullable", rs.getString("is_nullable"));
                columnInfo.put("character_maximum_length", rs.getObject("character_maximum_length"));
                return columnInfo;
            });
        } else {
            return jdbcTemplate.query(query, new Object[]{dbName, tableName}, (rs, rowNum) -> {
                Map<String, Object> columnInfo = new HashMap<>();
                columnInfo.put("column_name", rs.getString("column_name"));
                columnInfo.put("data_type", rs.getString("data_type"));
                columnInfo.put("is_nullable", rs.getString("is_nullable"));
                columnInfo.put("character_maximum_length", rs.getObject("character_maximum_length"));
                return columnInfo;
            });
        }
    }

}
