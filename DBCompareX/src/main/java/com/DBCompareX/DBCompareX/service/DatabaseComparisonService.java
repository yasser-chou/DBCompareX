package com.DBCompareX.DBCompareX.service;


import com.DBCompareX.DBCompareX.dao.entities.DatabaseConnector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DatabaseComparisonService {

    @Autowired
    private DatabaseConnector databaseConnector;

    public boolean connectToDatabases(String srcDbType,String srcHost,int srcPort,String srcDbName,String srcUser,String srcPass,
                                      String tgtDbType,String tgtHost,int tgtPort,String tgtDbName,String tgtUser,String tgtPass){
        return databaseConnector.connectToDatabases(srcDbType,srcHost,srcPort,srcDbName,srcUser,srcPass,
                                                    tgtDbType,tgtHost,tgtPort,tgtDbName,tgtUser,tgtPass);
    }
}
