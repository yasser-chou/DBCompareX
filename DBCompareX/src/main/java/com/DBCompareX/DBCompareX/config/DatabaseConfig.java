package com.DBCompareX.DBCompareX.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

@Data
@Component
@ConfigurationProperties(prefix = "database")
public class DatabaseConfig {
    private Map<String, String> jdbcUrl;
    private Map<String, String> driver;
} 