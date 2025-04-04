package com.DBCompareX.DBCompareX.dao.entities;

import lombok.Data;


import java.util.List;

@Data
public class ComparisonRequest {
    private String sourceDbType;
    private String targetDbType;
    private String sourceHost;
    private int sourcePort;
    private String sourceDbName;
    private String sourceUsername;
    private String sourcePassword;
    private String targetHost;
    private int targetPort;
    private String targetDbName;
    private String targetUsername;
    private String targetPassword;
    private List<TableMapping> tableMappings;
}
