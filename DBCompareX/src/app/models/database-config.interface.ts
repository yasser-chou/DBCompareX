export interface DatabaseConfig {
  dbType: 'mysql' | 'postgresql' | 'oracle' | 'sqlserver';
  host: string;
  port: number;
  schema: string;
  username: string;
  password: string;
  table?: string;
}

export interface TableMapping {
  sourceTable: string;
  targetTable: string;
  keyColumns: string[];
  columnMappings: ColumnMapping[];
}

export interface ColumnMapping {
  sourceColumn: string;
  targetColumn: string;
  transformationType?: 'none' | 'trim' | 'uppercase' | 'lowercase' | 'custom';
  customTransformation?: string;
}

export interface ComparisonConfig {
  mode: 'exact' | 'fuzzy' | 'hash';
  ignoreNulls: boolean;
  trimWhitespace: boolean;
  similarityThreshold?: number;
  caseSensitive: boolean;
}

export interface ComparisonResult {
  matches: number;
  mismatches: number;
  unmatchedSource: number;
  unmatchedTarget: number;
  differences: RecordDifference[];
}

export interface RecordDifference {
  table: string;
  key: string;
  sourceRecord: any;
  targetRecord: any;
  differences: FieldDifference[];
}

export interface FieldDifference {
  field: string;
  sourceValue: any;
  targetValue: any;
} 