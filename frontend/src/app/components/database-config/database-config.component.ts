import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { MatSnackBar } from '@angular/material/snack-bar';
import { DatabaseService, DatabaseConfig } from '../../services/database.service';

@Component({
  selector: 'app-database-config',
  templateUrl: './database-config.component.html',
  styleUrls: ['./database-config.component.scss']
})
export class DatabaseConfigComponent implements OnInit {
  sourceTables: string[] = [];
  targetTables: string[] = [];
  sourceConnectionStatus: {success: boolean, message: string} | null = null;
  targetConnectionStatus: {success: boolean, message: string} | null = null;
  canProceed = false;
  isLoading = false;
  
  // Configuration objects for source and target
  sourceConfig?: DatabaseConfig;
  targetConfig?: DatabaseConfig;

  constructor(
    private dbService: DatabaseService,
    private snackBar: MatSnackBar,
    private router: Router
  ) {}

  ngOnInit(): void {
    // Load any saved configurations
    const savedConfig = this.dbService.getSavedConfig();
    if (savedConfig) {
      this.sourceConfig = savedConfig.source;
      this.targetConfig = savedConfig.target;
    }
  }

  handleSourceStatusChange(status: {success: boolean, message: string}): void {
    this.sourceConnectionStatus = status;
    this.checkCanProceed();
  }

  handleTargetStatusChange(status: {success: boolean, message: string}): void {
    this.targetConnectionStatus = status;
    this.checkCanProceed();
  }

  handleSourceTablesLoaded(tables: string[]): void {
    this.sourceTables = tables;
    this.checkCanProceed();
  }

  handleTargetTablesLoaded(tables: string[]): void {
    this.targetTables = tables;
    this.checkCanProceed();
  }

  checkCanProceed(): void {
    this.canProceed = 
      this.sourceConnectionStatus?.success === true && 
      this.targetConnectionStatus?.success === true &&
      this.sourceTables.length > 0 && 
      this.targetTables.length > 0;
  }

  proceedToComparisonConfig(): void {
    // Save the current configurations
    this.dbService.saveConfig({
      source: this.sourceConfig,
      target: this.targetConfig
    });
    this.router.navigate(['/comparison-config']);
  }

  // Method to update the source config from child component
  updateSourceConfig(config: DatabaseConfig): void {
    this.sourceConfig = config;
  }

  // Method to update the target config from child component
  updateTargetConfig(config: DatabaseConfig): void {
    this.targetConfig = config;
  }
}
