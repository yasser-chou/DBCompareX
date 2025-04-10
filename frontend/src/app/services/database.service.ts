import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

export interface DatabaseConfig{
  jdbcUrl :string;
  username:string;
  password:string;
  databaseType:string;
}

export interface TableInfo {
  tableName:string;
  columnCount:number;
  rowCount:number;
}

@Injectable({
  providedIn: 'root'
})
export class DatabaseService {
  private apiUrl = 'http://localhost:8080/api';

  constructor(private http: HttpClient) { }

  testConnection(config: DatabaseConfig): Observable<boolean> {
    return this.http.post<boolean>(`${this.apiUrl}/testConnection`, config);
  }

  getTables(config: DatabaseConfig): Observable<TableInfo[]> {
    return this.http.post<TableInfo[]>(`${this.apiUrl}/getTables`, config);
  }

  getTableSchema(config:DatabaseConfig,tableName:string):Observable<any[]>{
    return this.http.post<TableInfo[]>('${this.apiUrl}/table-schema', {...config,tableName});
  }

  compareTables(sourceConfig:DatabaseConfig,targetConfig:DatabaseConfig,tables:string[]):Observable<any[]>{
    return this.http.post<any[]>('${this.apiUrl}/compare-tables', {sourceConfig,targetConfig,tables});
  }


}
