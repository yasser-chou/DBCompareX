import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { DatabaseConfig } from './database.service';

export interface ColumnMapping{
  sourceColumn:string;
  targetColumn:string;
  iskey : boolean;
}

export interface TableMapping{
  sourceTable:string;
  targetTable:string;
  columnMappings:ColumnMapping[];
}

export interface ComparisonConfig {
  caseSensitive: boolean;
  ignoreWhitespace: boolean;
  ignoreNulls: boolean;
  stringSimilarityThreshold: number;
  numericTolerance: number;
  tableMappings: TableMapping[];
}

export interface ComparisonSummary {
  totalRecords: number;
  matchCount: number;
  mismatchCount: number;
  unmatchedACount: number;
  unmatchedBCount: number;
}

export interface ComparisonResult {
  id:string;
  timeStamp:string;
  summary: ComparisonSummary;
  matches:any[];
  mismatches:any[];
  unmatchedA:any[];
  unmatchedB:any[];
}

@Injectable({
  providedIn: 'root'
})
export class ComparisonService {
  private apiUrl = 'http://localhost:8080/api/compare';
  
  constructor(private http: HttpClient) { }
  
  compareDatabases(
    sourceConfig: DatabaseConfig,
    targetConfig: DatabaseConfig,
    comparisonConfig: ComparisonConfig): Observable<{comparisonId: string}> {
    return this.http.post<{comparisonId: string}>(`${this.apiUrl}/compare-selected-tables`, {
      sourceDbType: sourceConfig.dbType,
      sourceHost: sourceConfig.host,
      sourcePort: sourceConfig.port,
      sourceDbName: sourceConfig.schema,
      sourceUsername: sourceConfig.username,
      sourcePassword: sourceConfig.password,
      targetDbType: targetConfig.dbType,
      targetHost: targetConfig.host,
      targetPort: targetConfig.port,
      targetDbName: targetConfig.schema,
      targetUsername: targetConfig.password,
      tableMappings: comparisonConfig.tableMappings
    });
  }
  
  getComparisonResults(comparisonId: string): Observable<ComparisonResult> {
    return this.http.get<ComparisonResult>(`${this.apiUrl}/download?filePath=${comparisonId}`);
  }
  
  exportToExcel(comparisonId: string): Observable<Blob> {
    return this.http.get(`${this.apiUrl}/download?filePath=${comparisonId}`, {
      responseType: 'blob'
    });
  }
}
