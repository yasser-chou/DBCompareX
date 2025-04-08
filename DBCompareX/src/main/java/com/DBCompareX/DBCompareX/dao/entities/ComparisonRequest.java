package com.DBCompareX.DBCompareX.dao.entities;

import lombok.Data;

import java.util.List;
import javax.validation.constraints.NotBlank;

@Data
public class ComparisonRequest {
    @NotBlank(message = "Source database type is required")
    private String sourceDbType;

    @NotBlank(message = "Target database type is required")
    private String targetDbType;

    @NotBlank(message = "Source host is required")
    private String sourceHost;

    @NotBlank(message = "Target host is required")
    private String targetHost;

    private int sourcePort;
    private int targetPort;

    @NotBlank(message = "Source database name is required")
    private String sourceDbName;

    @NotBlank(message = "Target database name is required")
    private String targetDbName;

    @NotBlank(message = "Source username is required")
    private String sourceUsername;

    @NotBlank(message = "Target username is required")
    private String targetUsername;

    @NotBlank(message = "Source password is required")
    private String sourcePassword;

    @NotBlank(message = "Target password is required")
    private String targetPassword;

    private List<TableMapping> tableMappings; // For selected table comparison


}