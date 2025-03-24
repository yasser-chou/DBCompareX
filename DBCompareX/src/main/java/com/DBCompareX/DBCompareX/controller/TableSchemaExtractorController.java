package com.DBCompareX.DBCompareX.controller;


import com.DBCompareX.DBCompareX.service.TableSchemaExtractor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/schema")
public class TableSchemaExtractorController {

    private final TableSchemaExtractor tableSchemaExtractor;

    public TableSchemaExtractorController(TableSchemaExtractor tableSchemaExtractor){
        this.tableSchemaExtractor = tableSchemaExtractor;
    }

    @GetMapping("/tables")
    public List<String> getAllTables(@RequestParam String dbType, @RequestParam String host, @RequestParam int port,
                                     @RequestParam String dbName, @RequestParam String username,
                                     @RequestParam String password){

        return tableSchemaExtractor.getAllTables(dbType,host,port,dbName,username,password);

    }

    @GetMapping("/columns")
    public List<Map<String,Object>> getTableColumns(@RequestParam String dbType, @RequestParam String host, @RequestParam int port,
                                                    @RequestParam String dbName, @RequestParam String username,
                                                    @RequestParam String password, @RequestParam String tableName){

        return tableSchemaExtractor.getTableColumns(dbType,host,port,dbName,username,password,tableName);

    }
}
