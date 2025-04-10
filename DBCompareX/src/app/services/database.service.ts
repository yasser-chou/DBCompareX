import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { DatabaseConfig, TableMapping } from '../models/database-config.interface';

export interface TableInfo {
  tableName: string;
  columnCount: number;
  rowCount: number;
}

@Injectable({
  providedIn: 'root'
})
export class DatabaseService {
  private apiUrl = 'http://localhost:8080/api'; // Update with your backend URL

  constructor(private http: HttpClient) {}

  testConnection(config: DatabaseConfig): Observable<boolean> {
    return this.http.post<boolean>(`${this.apiUrl}/test-connection`, config);
  }

  getTables(config: DatabaseConfig): Observable<TableInfo[]> {
    return this.http.post<TableInfo[]>(`${this.apiUrl}/tables`, config);
  }

  getTableSchema(config: DatabaseConfig, tableName: string): Observable<any> {
    return this.http.post<any>(`${this.apiUrl}/table-schema`, {
      ...config,
      tableName
    });
  }

  compareTables(sourceConfig: DatabaseConfig, targetConfig: DatabaseConfig, tables: string[]): Observable<any> {
    return this.http.post<any>(`${this.apiUrl}/compare-tables`, {
      sourceConfig,
      targetConfig,
      tables
    });
  }

  fetchTableNames(config: DatabaseConfig): Observable<string[]> {
    return this.http.post<string[]>(`${this.apiUrl}/fetch-table-names`, config);
  }

  fetchTableMetadata(config: DatabaseConfig, tableName: string): Observable<any> {
    return this.http.post<any>(`${this.apiUrl}/fetch-table-metadata`, { ...config, table: tableName });
  }

  findCommonTables(sourceConfig: DatabaseConfig, targetConfig: DatabaseConfig): Observable<TableMapping[]> {
    return this.http.post<TableMapping[]>(`${this.apiUrl}/find-common-tables`, {
      source: sourceConfig,
      target: targetConfig
    });
  }

  getAvailableTables(sourceConfig: DatabaseConfig, targetConfig: DatabaseConfig): Observable<TableMapping[]> {
    return this.http.post<TableMapping[]>(`${this.apiUrl}/get-available-tables`, {
      source: sourceConfig,
      target: targetConfig
    });
  }
} 