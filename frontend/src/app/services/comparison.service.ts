import { Injectable } from '@angular/core';
import { HttpClient} from '@angular/common/http';
import { Observable} from 'rxjs';
import { DatabaseConfig} from './database.service';


export interface ComparisonConfig{
  caseSensitive:boolean;
  ignoreWhitespace:boolean;
  ignoreNulls:boolean;
  stringSimilarityThreshold:number;
  numericTolerance:number;
  tableMappings:TableMapping[];
  source:DatabaseConfig;
  target:DatabaseConfig;
  tables:string[];
}


export interface TableMapping{
  sourceTable:string;
  targetTable:string;
  columnMappings:ColumnMapping[];
}

export interface ColumnMapping{
  sourceColumn:string;
  targetColumn:string;
  transformationType:string;
}

@Injectable({
  providedIn: 'root'
})
export class ComparisonService {
  private apiUrl = 'http://localhost:8080/api';
  constructor(private http: HttpClient) { }

  compareDatabasess(
    sourceConfig:DatabaseConfig,
    targetConfig:DatabaseConfig,
    comparisonConfig:ComparisonConfig): Observable<any>{
      return this.http.post<any>(`${this.apiUrl}/compare-databases`,{
        sourceConfig,
        targetConfig,
        comparisonConfig
      });
     }

     getComparisonResults(comparisonId:string):Observable<any>{
       return this.http.get<any>(`${this.apiUrl}/get-comparison-results/${comparisonId}`);
     }

}
