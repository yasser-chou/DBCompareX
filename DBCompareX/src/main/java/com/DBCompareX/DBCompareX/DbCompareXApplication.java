package com.DBCompareX.DBCompareX;

import com.DBCompareX.DBCompareX.config.DatabaseConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication(scanBasePackages = "com.DBCompareX.DBCompareX")
@EnableConfigurationProperties(DatabaseConfig.class)
public class DbCompareXApplication {

	public static void main(String[] args) {
		SpringApplication.run(DbCompareXApplication.class, args);
	}

}
