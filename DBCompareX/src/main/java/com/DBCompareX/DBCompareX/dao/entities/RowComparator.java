package com.DBCompareX.DBCompareX.dao.entities;

import java.util.*;
import java.util.stream.Collectors;

public class RowComparator {

    /**
     * Compares rows from source and target databases using dynamically generated composite keys.
     */
    public Map<String, List<Map<String, Object>>> compareRows(
            List<Map<String, Object>> srcRows, List<Map<String, Object>> tgtRows) {

        Map<String, List<Map<String, Object>>> comparisonResult = new HashMap<>();

        Map<String, Map<String, Object>> srcKeyMap = generateCompositeKeys(srcRows);
        Map<String, Map<String, Object>> tgtKeyMap = generateCompositeKeys(tgtRows);

        System.out.println("Source rows: " + srcRows.size()); // Log source rows
        System.out.println("Target rows: " + tgtRows.size()); // Log target rows

        List<Map<String, Object>> identicalRecords = new ArrayList<>();
        List<Map<String, Object>> unmatchedSrcRecords = new ArrayList<>(srcRows);
        List<Map<String, Object>> unmatchedTgtRecords = new ArrayList<>(tgtRows);
        List<Map<String, Object>> differences = new ArrayList<>();

        for (Map.Entry<String, Map<String, Object>> srcEntry : srcKeyMap.entrySet()) {
            String srcKey = srcEntry.getKey();
            Map<String, Object> srcRow = srcEntry.getValue();

            Map<String, Object> tgtRow = tgtKeyMap.get(srcKey);
            if (tgtRow != null) {
                unmatchedSrcRecords.remove(srcRow);
                unmatchedTgtRecords.remove(tgtRow);

                if (areRowsIdentical(srcRow, tgtRow)) {
                    identicalRecords.add(srcRow);
                } else {
                    differences.add(createDifferenceRecord(srcRow, tgtRow));
                }
            }
        }

        System.out.println("Identical records: " + identicalRecords.size()); // Log identical records
        System.out.println("Unmatched source records: " + unmatchedSrcRecords.size()); // Log unmatched source records
        System.out.println("Unmatched target records: " + unmatchedTgtRecords.size()); // Log unmatched target records
        System.out.println("Differences: " + differences.size()); // Log differences

        comparisonResult.put("identical", identicalRecords);
        comparisonResult.put("unmatched_src", unmatchedSrcRecords);
        comparisonResult.put("unmatched_tgt", unmatchedTgtRecords);
        comparisonResult.put("differences", differences);

        return comparisonResult;
    }

    /**
     * Generates composite keys for rows dynamically.
     */
    private Map<String, Map<String, Object>> generateCompositeKeys(List<Map<String, Object>> rows) {
        Map<String, Map<String, Object>> compositeKeyMap = new HashMap<>();
        Set<String> candidateColumns = identifyCandidateColumns(rows);
        System.out.println("Candidate columns for composite key: " + candidateColumns); // Log candidate columns

        for (Map<String, Object> row : rows) {
            String compositeKey = generateCompositeKeyForRow(row, candidateColumns);
            System.out.println("Generated composite key: " + compositeKey); // Log composite key
            compositeKeyMap.put(compositeKey, row);
        }

        return compositeKeyMap;
    }

    /**
     * Identifies candidate columns for forming a composite key.
     */
    private Set<String> identifyCandidateColumns(List<Map<String, Object>> rows) {
        Set<String> candidateColumns = new HashSet<>();
        Set<String> allColumns = rows.get(0).keySet();

        for (String column : allColumns) {
            Set<Object> uniqueValues = rows.stream()
                    .map(row -> normalizeValue(row.get(column)))
                    .collect(Collectors.toSet());

            double uniquenessRatio = (double) uniqueValues.size() / rows.size();
            if (uniquenessRatio > 0.8) { // Heuristic: 80% unique values
                candidateColumns.add(column);
            }
        }

        if (candidateColumns.isEmpty()) {
            candidateColumns.addAll(allColumns);
        }

        return candidateColumns;
    }

    /**
     * Generates a composite key for a single row.
     */
    private String generateCompositeKeyForRow(Map<String, Object> row, Set<String> candidateColumns) {
        return candidateColumns.stream()
                .map(column -> normalizeValue(row.get(column)).toString())
                .collect(Collectors.joining("|"));
    }

    /**
     * Normalizes a value to ensure consistency in composite key generation.
     */
    private Object normalizeValue(Object value) {
        if (value instanceof String) {
            return ((String) value).trim().toLowerCase();
        }
        return value != null ? value : "";
    }

    /**
     * Checks if two rows are identical by comparing all fields.
     */
    private boolean areRowsIdentical(Map<String, Object> srcRow, Map<String, Object> tgtRow) {
        return srcRow.entrySet().stream()
                .allMatch(entry -> Objects.equals(entry.getValue(), tgtRow.get(entry.getKey())));
    }

    /**
     * Creates a record detailing the differences between two rows.
     */
    private Map<String, Object> createDifferenceRecord(Map<String, Object> srcRow, Map<String, Object> tgtRow) {
        Map<String, Object> differenceRecord = new HashMap<>();
        differenceRecord.put("src_row", srcRow);
        differenceRecord.put("tgt_row", tgtRow);

        Map<String, Map<String, Object>> differingFields = new HashMap<>();
        Set<String> allFields = new HashSet<>(srcRow.keySet());
        allFields.addAll(tgtRow.keySet());

        for (String field : allFields) {
            Object srcValue = srcRow.get(field);
            Object tgtValue = tgtRow.get(field);

            if (!Objects.equals(srcValue, tgtValue)) {
                differingFields.put(field, Map.of(
                        "src_value", srcValue,
                        "tgt_value", tgtValue
                ));
            }
        }

        differenceRecord.put("differing_fields", differingFields);
        return differenceRecord;
    }
}
