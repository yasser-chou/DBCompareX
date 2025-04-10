//package com.DBCompareX.DBCompareX;
//
//import org.apache.spark.sql.SparkSession;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.apache.hadoop.security.UserGroupInformation;
//
//@Configuration
//public class SparkConfig {
//    private static final Logger logger = LoggerFactory.getLogger(SparkConfig.class);
//
//    @Bean
//    public SparkSession sparkSession() {
//        try {
//            // Set Hadoop home directory
//            String hadoopDir = "C:\\Users\\Hp\\Desktop\\hadoop-3.4.1";
//            System.setProperty("hadoop.home.dir", hadoopDir);
//            System.setProperty("HADOOP_HOME", hadoopDir);
//            logger.info("Hadoop home directory set to: {}", hadoopDir);
//
//            // Configure security manager settings
//            System.setProperty("java.security.manager", "allow");
//            System.setProperty("java.security.policy", "");
//            System.setProperty("spark.security.manager.enabled", "false");
//            System.setProperty("spark.driver.allowMultipleContexts", "true");
//
//            // Configure Hadoop security
//            org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();
//            hadoopConfig.set("hadoop.security.authentication", "simple");
//            hadoopConfig.set("hadoop.security.authorization", "false");
//            hadoopConfig.set("fs.defaultFS", "file:///");
//
//            // Initialize Hadoop security with system user
//            UserGroupInformation.setConfiguration(hadoopConfig);
//            UserGroupInformation.createRemoteUser("system");
//
//            // Create SparkSession with optimized configurations
//            return SparkSession.builder()
//                    .appName("DBCompareX")
//                    .master("local[*]")
//                    .config("spark.sql.shuffle.partitions", "4")
//                    .config("spark.driver.memory", "512m")
//                    .config("spark.executor.memory", "512m")
//                    .config("spark.driver.host", "localhost")
//                    .config("spark.driver.bindAddress", "localhost")
//                    .config("spark.hadoop.fs.defaultFS", "file:///")
//                    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
//                    .config("spark.hadoop.hadoop.security.authentication", "simple")
//                    .config("spark.hadoop.hadoop.security.authorization", "false")
//                    .config("spark.driver.allowMultipleContexts", "true")
//                    .config("spark.driver.user.name", "system")
//                    .config("spark.executor.user.name", "system")
//                    .getOrCreate();
//        } catch (Exception e) {
//            logger.error("Failed to create SparkSession: {}", e.getMessage(), e);
//            throw new RuntimeException("Failed to initialize Spark session", e);
//        }
//    }
//}