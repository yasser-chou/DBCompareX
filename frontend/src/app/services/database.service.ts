import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, BehaviorSubject, of } from 'rxjs';
import { map, catchError } from 'rxjs/operators';

export interface DatabaseConfig {
  dbType: string;
  host: string;
  port: string;
  username: string;
  password: string;
  schema: string;
  databaseName?: string;
  table?: string;
}

export interface ComparisonRequest {
  sourceDbType: string;
  sourceHost: string;
  sourcePort: string;
  sourceDbName: string;
  sourceUsername: string;
  sourcePassword: string;
  targetDbType: string;
  targetHost: string;
  targetPort: string;
  targetDbName: string;
  targetUsername: string;
  targetPassword: string;
  sourceSchemaFilter?: string;
  targetSchemaFilter?: string;
  maxTables?: number;
  sourceTableFilterSQL?: string;
  targetTableFilterSQL?: string;
  specificTables?: string[];
  skipTableDiscovery?: boolean;
  tableMappings?: TableMapping[];
}

export interface TableMapping {
  sourceTable: string;
  targetTable: string;
  columnMappings?: ColumnMapping[];
}

export interface ColumnMapping {
  sourceColumn: string;
  targetColumn: string;
  iskey: boolean;
}

export interface ConnectionStatus{
  success:boolean;
  message:string;
}

@Injectable({
  providedIn: 'root'
})
export class DatabaseService {
  private apiUrl = 'http://localhost:8080/api/compare';
  private configSource = new BehaviorSubject<{source?: DatabaseConfig, target?: DatabaseConfig}>({});
  currentConfig = this.configSource.asObservable();

  constructor(private http: HttpClient) { }

  testConnection(config: DatabaseConfig, type: 'source' | 'target'): Observable<boolean> {
    // Create a request that has both source and target (same values for testing)
    const request: ComparisonRequest = {
      sourceDbType: config.dbType,
      sourceHost: config.host,
      sourcePort: config.port,
      sourceDbName: config.schema, // For Oracle, this is the service name (e.g., xepdb1)
      sourceUsername: config.username, // Use the actual username credentials
      sourcePassword: config.password,
      targetDbType: config.dbType,
      targetHost: config.host,
      targetPort: config.port,
      targetDbName: config.schema, // For Oracle, this is the service name (e.g., xepdb1)
      targetUsername: config.username, // Use the actual username credentials
      targetPassword: config.password,
      maxTables: 50 // Limit the number of tables to process
    };

    // Add schema filter if available for Oracle
    if (config.dbType === 'oracle' && config.databaseName) {
      if (type === 'source') {
        request.sourceSchemaFilter = config.databaseName.toUpperCase(); // Oracle schema names are typically uppercase
      } else {
        request.targetSchemaFilter = config.databaseName.toUpperCase(); // Oracle schema names are typically uppercase
      }
    }

    // Use the get-available-tables endpoint to test connection
    return this.http.post<any>(`${this.apiUrl}/get-available-tables`, request)
      .pipe(
        map(response => {
          // If we get here, the connection was successful
          console.log(`${type} connection success:`, response);
          return true;
        }),
        catchError(error => {
          console.error(`${type} connection error:`, error);
          // Return false instead of throwing an error
          return of(false);
        })
      );
  }

  getTables(config: DatabaseConfig): Observable<string[]> {
    // Create a request with both source and target (same values for simplicity)
    const request: ComparisonRequest = {
      sourceDbType: config.dbType,
      sourceHost: config.host,
      sourcePort: config.port,
      sourceDbName: config.schema, // For Oracle, this is the service name (e.g., xepdb1)
      sourceUsername: config.username, // Use the actual username credentials
      sourcePassword: config.password,
      targetDbType: config.dbType,
      targetHost: config.host,
      targetPort: config.port,
      targetDbName: config.schema, // For Oracle, this is the service name (e.g., xepdb1)
      targetUsername: config.username, // Use the actual username credentials
      targetPassword: config.password,
      maxTables: 50 // Limit the number of tables to process
    };

    // Add schema filter if available for Oracle
    if (config.dbType === 'oracle' && config.databaseName) {
      request.sourceSchemaFilter = config.databaseName.toUpperCase(); // Oracle schema names are typically uppercase
      request.targetSchemaFilter = config.databaseName.toUpperCase(); // Oracle schema names are typically uppercase

      // For Oracle, we can also modify the SQL query via special params
      request.sourceTableFilterSQL = `AND OWNER = '${config.databaseName.toUpperCase()}'`;
      request.targetTableFilterSQL = `AND OWNER = '${config.databaseName.toUpperCase()}'`;

      // If it's Oracle, use an alternative approach: force usage of a separate endpoint
      return this.getSpecificTables(config);
    }

    return this.http.post<any>(`${this.apiUrl}/get-available-tables`, request)
      .pipe(
        map(response => {
          console.log('Get tables response:', response);
          if (response.commonTables && Array.isArray(response.commonTables)) {
            // Extract table names from the format "sourceTable -> targetTable"
            return response.commonTables.map((tableStr: string) => {
              return tableStr.split(' -> ')[0].trim();
            });
          }
          return [];
        }),
        catchError(error => {
          console.error('Error getting tables:', error);
          throw error;
        })
      );
  }

  // New method specifically for Oracle to get filtered tables
  getSpecificTables(config: DatabaseConfig): Observable<string[]> {
    // Get tables directly using a simple query from USER_TABLES or ALL_TABLES view
    const query = `
      SELECT table_name
      FROM all_tables
      WHERE owner = '${config.databaseName?.toUpperCase()}'
    `;

    const request = {
      sourceDbType: config.dbType,
      sourceHost: config.host,
      sourcePort: config.port,
      sourceDbName: config.schema,
      sourceUsername: config.username,
      sourcePassword: config.password,
      query: query
    };

    // Use a custom SQL query endpoint if available, or fallback to a simplified version
    return this.http.post<any>(`${this.apiUrl}/execute-query`, request)
      .pipe(
        map(response => {
          console.log('Direct query response:', response);
          // Extract table names from the response
          if (response.results && Array.isArray(response.results)) {
            return response.results.map((row: any) => {
              // Handle different column name formats that might be returned
              const tableName = row.TABLE_NAME || row.table_name || row.TABLENAME ||
                row[Object.keys(row)[0]]; // Fallback to first column if none match
              return typeof tableName === 'string' ? tableName.toLowerCase() : tableName;
            }).filter(Boolean); // Filter out undefined/null values
          }
          return [];
        }),
        catchError(error => {
          console.error('Error executing direct query:', error);
          // Make a second attempt with user_tables instead of all_tables
          const fallbackQuery = `
          SELECT table_name
          FROM user_tables
        `;

          const fallbackRequest = {
            ...request,
            query: fallbackQuery
          };

          return this.http.post<any>(`${this.apiUrl}/execute-query`, fallbackRequest)
            .pipe(
              map(response => {
                console.log('Fallback query response:', response);
                if (response.results && Array.isArray(response.results)) {
                  return response.results.map((row: any) => {
                    const tableName = row.TABLE_NAME || row.table_name || row.TABLENAME ||
                      row[Object.keys(row)[0]];
                    return typeof tableName === 'string' ? tableName.toLowerCase() : tableName;
                  }).filter(Boolean);
                }
                return [];
              }),
              catchError(err => {
                console.error('Fallback query also failed:', err);
                // As a last resort, provide some common tables that might exist
                return of([]);
              })
            );
        })
      );
  }

  getAvailableTables(sourceConfig: DatabaseConfig, targetConfig: DatabaseConfig): Observable<TableMapping[]> {
    const request: ComparisonRequest = {
      sourceDbType: sourceConfig.dbType,
      sourceHost: sourceConfig.host,
      sourcePort: sourceConfig.port,
      sourceDbName: sourceConfig.schema, // For Oracle, this is the service name (e.g., xepdb1)
      sourceUsername: sourceConfig.username, // Use the actual username credentials
      sourcePassword: sourceConfig.password,
      targetDbType: targetConfig.dbType,
      targetHost: targetConfig.host,
      targetPort: targetConfig.port,
      targetDbName: targetConfig.schema, // For Oracle, this is the service name (e.g., xepdb1)
      targetUsername: targetConfig.username, // Use the actual username credentials
      targetPassword: targetConfig.password,
      maxTables: 50 // Limit the number of tables to process
    };

    // Add schema filter if available for Oracle
    if (sourceConfig.dbType === 'oracle' && sourceConfig.databaseName) {
      request.sourceSchemaFilter = sourceConfig.databaseName.toUpperCase(); // Oracle schema names are typically uppercase
      request.sourceTableFilterSQL = `AND OWNER = '${sourceConfig.databaseName.toUpperCase()}'`;
    }
    if (targetConfig.dbType === 'oracle' && targetConfig.databaseName) {
      request.targetSchemaFilter = targetConfig.databaseName.toUpperCase(); // Oracle schema names are typically uppercase
      request.targetTableFilterSQL = `AND OWNER = '${targetConfig.databaseName.toUpperCase()}'`;
    }

    return this.http.post<any>(`${this.apiUrl}/get-available-tables`, request)
      .pipe(
        map(response => {
          console.log('Available tables response:', response);
          if (response.commonTables && Array.isArray(response.commonTables)) {
            // Convert string format to TableMapping objects
            return response.commonTables.map((tableStr: string) => {
              const parts = tableStr.split(' -> ');
              return {
                sourceTable: parts[0].trim(),
                targetTable: parts[1].trim()
              };
            });
          }
          return [];
        })
      );
  }

  saveConfig(config: {source?: DatabaseConfig, target?: DatabaseConfig}): void {
    this.configSource.next(config);
    localStorage.setItem('dbConfig', JSON.stringify(config));
  }

  getSavedConfig(): {source?: DatabaseConfig, target?: DatabaseConfig} {
    const saved = localStorage.getItem('dbConfig');
    return saved ? JSON.parse(saved) : {};
  }
}
