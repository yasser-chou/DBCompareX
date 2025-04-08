package com.DBCompareX.DBCompareX.util;

import com.DBCompareX.DBCompareX.dao.entities.ExcelGenerator;
import com.DBCompareX.DBCompareX.dao.entities.TableMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.*;

@Component
public class ExcelReportGenerator {
    private static final Logger logger = LoggerFactory.getLogger(ExcelReportGenerator.class);

    @Autowired
    private ExcelGenerator excelGenerator;

    /**
     * Generates an Excel report comparing the data from two databases
     * 
     * @param comparisonResults The results from database comparison
     * @param outputPath The path where the Excel file should be saved
     * @return The generated Excel file
     */
    public File generateReport(Map<String, Object> comparisonResults, String outputPath) {
        // Create table mappings
        List<TableMapping> tableMappings = createTableMappings();
        
        // Generate the Excel report
        return excelGenerator.generateExcelReport(comparisonResults, outputPath, tableMappings);
    }
    
    /**
     * Creates table mappings for database comparison
     * Customize this method based on your database schema
     */
    private List<TableMapping> createTableMappings() {
        List<TableMapping> mappings = new ArrayList<>();
        
        // Customer mapping - match by email, first_name, last_name
        TableMapping customerMapping = new TableMapping("customers", "customers");
        customerMapping.setKeyColumns(Arrays.asList("email", "first_name", "last_name"));
        
        // Transaction mapping - match by reference_number
        TableMapping transactionMapping = new TableMapping("transactions", "transactions");
        transactionMapping.setKeyColumns(Arrays.asList("reference_number"));
        
        mappings.add(customerMapping);
        mappings.add(transactionMapping);
        
        return mappings;
    }
    
    /**
     * Prepares comparison results from your database data
     * This method properly formats the data for the Excel generator
     */
    public Map<String, Object> prepareComparisonResults(List<Map<String, Object>> db1Records, 
                                                       List<Map<String, Object>> db2Records) {
        Map<String, Object> results = new HashMap<>();
        List<String> differences = new ArrayList<>();
        List<String> unmatchedSource = new ArrayList<>();
        List<String> unmatchedTarget = new ArrayList<>();
        int exactMatches = 0;
        
        // Create maps for faster lookup
        Map<String, Map<String, Object>> db1CustomerMap = new HashMap<>();
        Map<String, Map<String, Object>> db2CustomerMap = new HashMap<>();
        Map<String, Map<String, Object>> db1TransactionMap = new HashMap<>();
        Map<String, Map<String, Object>> db2TransactionMap = new HashMap<>();
        
        // Organize records by type and key
        for (Map<String, Object> record : db1Records) {
            if (record.containsKey("transaction_id")) {
                String refNum = (String) record.get("reference_number");
                db1TransactionMap.put(refNum, record);
            } else {
                String email = (String) record.get("email");
                db1CustomerMap.put(email, record);
            }
        }
        
        for (Map<String, Object> record : db2Records) {
            if (record.containsKey("transaction_id")) {
                String refNum = (String) record.get("reference_number");
                db2TransactionMap.put(refNum, record);
            } else {
                String email = (String) record.get("email");
                db2CustomerMap.put(email, record);
            }
        }
        
        // Compare customers
        for (String email : db1CustomerMap.keySet()) {
            Map<String, Object> db1Customer = db1CustomerMap.get(email);
            
            if (db2CustomerMap.containsKey(email)) {
                Map<String, Object> db2Customer = db2CustomerMap.get(email);
                boolean hasDifference = false;
                
                // Create a difference record
                Map<String, Object> diffRecord = new HashMap<>();
                
                // Add source and target fields with prefixes
                for (String key : db1Customer.keySet()) {
                    diffRecord.put("src_" + key, db1Customer.get(key));
                }
                
                for (String key : db2Customer.keySet()) {
                    diffRecord.put("tgt_" + key, db2Customer.get(key));
                    
                    // Check for differences
                    if (db1Customer.containsKey(key) && 
                        !Objects.equals(db1Customer.get(key), db2Customer.get(key))) {
                        hasDifference = true;
                    }
                }
                
                if (hasDifference) {
                    differences.add(convertToJson(diffRecord));
                } else {
                    exactMatches++;
                }
                
                // Remove from db2 map to track unmatched
                db2CustomerMap.remove(email);
            } else {
                // Unmatched in source
                unmatchedSource.add(convertToJson(db1Customer));
            }
        }
        
        // Add remaining db2 customers as unmatched target
        for (Map<String, Object> customer : db2CustomerMap.values()) {
            unmatchedTarget.add(convertToJson(customer));
        }
        
        // Compare transactions
        for (String refNum : db1TransactionMap.keySet()) {
            Map<String, Object> db1Transaction = db1TransactionMap.get(refNum);
            
            if (db2TransactionMap.containsKey(refNum)) {
                Map<String, Object> db2Transaction = db2TransactionMap.get(refNum);
                boolean hasDifference = false;
                
                // Create a difference record
                Map<String, Object> diffRecord = new HashMap<>();
                
                // Add source and target fields with prefixes
                for (String key : db1Transaction.keySet()) {
                    diffRecord.put("src_" + key, db1Transaction.get(key));
                }
                
                for (String key : db2Transaction.keySet()) {
                    diffRecord.put("tgt_" + key, db2Transaction.get(key));
                    
                    // Check for differences
                    if (db1Transaction.containsKey(key) && 
                        !Objects.equals(db1Transaction.get(key), db2Transaction.get(key))) {
                        hasDifference = true;
                    }
                }
                
                if (hasDifference) {
                    differences.add(convertToJson(diffRecord));
                } else {
                    exactMatches++;
                }
                
                // Remove from db2 map to track unmatched
                db2TransactionMap.remove(refNum);
            } else {
                // Unmatched in source
                unmatchedSource.add(convertToJson(db1Transaction));
            }
        }
        
        // Add remaining db2 transactions as unmatched target
        for (Map<String, Object> transaction : db2TransactionMap.values()) {
            unmatchedTarget.add(convertToJson(transaction));
        }
        
        results.put("differences", differences);
        results.put("unmatched_source", unmatchedSource);
        results.put("unmatched_target", unmatchedTarget);
        results.put("exact_matches", exactMatches);
        
        return results;
    }
    
    /**
     * Converts a Map to a JSON string
     */
    private String convertToJson(Map<String, Object> map) {
        StringBuilder json = new StringBuilder("{");
        boolean first = true;
        
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!first) {
                json.append(",");
            }
            first = false;
            
            json.append("\"").append(entry.getKey()).append("\":");
            
            if (entry.getValue() == null) {
                json.append("null");
            } else if (entry.getValue() instanceof Number) {
                json.append(entry.getValue());
            } else {
                json.append("\"").append(entry.getValue()).append("\"");
            }
        }
        
        json.append("}");
        return json.toString();
    }
    
    /**
     * Creates sample comparison results based on the provided MySQL and Oracle data
     * This method can be used for testing
     */
    public Map<String, Object> createSampleComparisonResults() {
        Map<String, Object> results = new HashMap<>();
        List<String> differences = new ArrayList<>();
        List<String> unmatchedSource = new ArrayList<>();
        List<String> unmatchedTarget = new ArrayList<>();
        
        // Sample differences based on your data
        differences.add("{\"src_id\":\"3\",\"src_first_name\":\"Michael\",\"src_last_name\":\"Johnson\",\"src_email\":\"michael.j@example.com\",\"tgt_id\":\"203\",\"tgt_first_name\":\"Michael\",\"tgt_last_name\":\"Johnson\",\"tgt_email\":\"michael.johnson@example.com\"}");
        differences.add("{\"src_id\":\"4\",\"src_first_name\":\"Emily\",\"src_last_name\":\"Williams\",\"src_email\":\"emily.w@example.com\",\"src_city\":\"Houston\",\"tgt_id\":\"204\",\"tgt_first_name\":\"Emily\",\"tgt_last_name\":\"Williams\",\"tgt_email\":\"emily.w@example.com\",\"tgt_city\":\"Austin\"}");
        differences.add("{\"src_transaction_id\":\"102\",\"src_customer_id\":\"2\",\"src_amount\":\"175.50\",\"src_status\":\"completed\",\"src_reference_number\":\"REF-002-2023\",\"tgt_transaction_id\":\"2002\",\"tgt_customer_id\":\"202\",\"tgt_amount\":\"175.50\",\"tgt_status\":\"pending\",\"tgt_reference_number\":\"REF-002-2023\"}");
        differences.add("{\"src_transaction_id\":\"108\",\"src_customer_id\":\"2\",\"src_amount\":\"310.25\",\"src_reference_number\":\"REF-008-2023\",\"tgt_transaction_id\":\"2008\",\"tgt_customer_id\":\"202\",\"tgt_amount\":\"315.25\",\"tgt_reference_number\":\"REF-008-2023\"}");
        
        // Sample unmatched source records
        unmatchedSource.add("{\"id\":\"6\",\"first_name\":\"Sarah\",\"last_name\":\"Miller\",\"email\":\"sarah.m@example.com\",\"phone\":\"555-345-6789\",\"city\":\"Philadelphia\",\"state\":\"PA\",\"country\":\"USA\"}");
        unmatchedSource.add("{\"transaction_id\":\"106\",\"customer_id\":\"6\",\"amount\":\"125.50\",\"status\":\"completed\",\"reference_number\":\"REF-006-2023\"}");
        
        // Sample unmatched target records
        unmatchedTarget.add("{\"id\":\"207\",\"first_name\":\"Robert\",\"last_name\":\"Taylor\",\"email\":\"robert.t@example.com\",\"phone\":\"555-876-5432\",\"city\":\"Seattle\",\"state\":\"WA\",\"country\":\"USA\"}");
        unmatchedTarget.add("{\"transaction_id\":\"2009\",\"customer_id\":\"207\",\"amount\":\"175.00\",\"status\":\"completed\",\"reference_number\":\"REF-009-2023\"}");
        
        results.put("differences", differences);
        results.put("unmatched_source", unmatchedSource);
        results.put("unmatched_target", unmatchedTarget);
        results.put("exact_matches", 8); // 3 customer matches + 5 transaction matches
        
        return results;
    }
}