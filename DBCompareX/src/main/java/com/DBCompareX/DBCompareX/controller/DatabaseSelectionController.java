package com.DBCompareX.DBCompareX.controller;

import com.DBCompareX.DBCompareX.service.DatabaseComparisonService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/databases")
public class DatabaseSelectionController {

    @Autowired
    private DatabaseComparisonService databaseComparisonService;

    //create api to connect to two databases dynamically

    @PostMapping("/connect")
    public String connectToDatabases(@RequestParam String srcDbType,@RequestParam String srcHost,
                                     @RequestParam int srcPort,@RequestParam String srcDbName, @RequestParam String srcUser,@RequestParam String srcPass,
                                     @RequestParam String tgtDbType,@RequestParam String tgtHost,@RequestParam int tgtPort,@RequestParam String tgtDbName,
                                     @RequestParam String tgtUser,@RequestParam String tgtPass){
        boolean success = databaseComparisonService.connectToDatabases(
                srcDbType,srcHost,srcPort,srcDbName,srcUser,srcPass,
                tgtDbType,tgtHost,tgtPort,tgtDbName,tgtUser,tgtPass
        );

        return success ? "connected to both databases !" : "connection failed !";
    }
}
