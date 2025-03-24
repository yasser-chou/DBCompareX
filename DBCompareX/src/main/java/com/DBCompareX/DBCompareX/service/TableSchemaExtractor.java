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

    public List<String> getAllTables(String dbType,String host,int port,String dbName,String username,String password){
        List<String> tables = new ArrayList<>();

        try (Connection connection = databaseConnector.getDataSource(dbType,host,port,dbName,username,password).getConnection()) {
            String query = "SELECT table_name FROM information_schema.tables WHERE table_schema=?";
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                statement.setString(1, dbName);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        tables.add(resultSet.getString("table_name"));
                    }
                }
            }
        }catch (SQLException e){
            throw new RuntimeException("Error retrieving tables" +e.getMessage(),e);
        }


        return tables;

    }



    //Get the table columns:
    public List<Map<String, Object>> getTableColumns(String dbType, String host, int port, String dbName, String username, String password, String tableName) {
        DataSource dataSource = databaseConnector.getDataSource(dbType, host, port, dbName, username, password);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String query = "SELECT column_name, data_type, is_nullable, character_maximum_length " +
                "FROM information_schema.columns WHERE table_schema = ? AND table_name = ?";

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
