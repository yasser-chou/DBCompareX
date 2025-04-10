import { Component } from '@angular/core';
import { MatSnackBar } from '@angular/material/snack-bar';
import { DatabaseConfig, ComparisonConfig, ComparisonResult } from './models/database-config.interface';
import { DatabaseService } from './services/database.service';
import { ComparisonService } from './services/comparison.service';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent {
  sourceConfig: DatabaseConfig | null = null;
  targetConfig: DatabaseConfig | null = null;
  comparisonConfig: ComparisonConfig | null = null;
  comparisonResults: ComparisonResult | null = null;
  showResults = false;
  isLoading = false;

  constructor(
    private databaseService: DatabaseService,
    private comparisonService: ComparisonService,
    private snackBar: MatSnackBar
  ) {}

  get canCompare(): boolean {
    return !!(this.sourceConfig && this.targetConfig && this.comparisonConfig);
  }

  onSourceConfigChange(config: DatabaseConfig): void {
    this.sourceConfig = config;
    this.showResults = false;
  }

  onTargetConfigChange(config: DatabaseConfig): void {
    this.targetConfig = config;
    this.showResults = false;
  }

  onComparisonConfigChange(config: ComparisonConfig): void {
    this.comparisonConfig = config;
    this.showResults = false;
  }

  async startComparison(): Promise<void> {
    if (!this.canCompare) return;

    this.isLoading = true;
    this.showResults = false;

    try {
      // First test the connections
      const sourceConnected = await this.databaseService.testConnection(this.sourceConfig!).toPromise();
      const targetConnected = await this.databaseService.testConnection(this.targetConfig!).toPromise();

      if (!sourceConnected || !targetConnected) {
        throw new Error('Failed to connect to one or both databases');
      }

      // Get common tables
      const tableMappings = await this.databaseService.findCommonTables(this.sourceConfig!, this.targetConfig!).toPromise();

      if (!tableMappings?.length) {
        throw new Error('No common tables found between the databases');
      }

      // Start comparison
      this.comparisonResults = await this.comparisonService.compareSelectedTables(
        this.sourceConfig!,
        this.targetConfig!,
        tableMappings,
        this.comparisonConfig!
      ).toPromise();

      this.showResults = true;
      this.snackBar.open('Comparison completed successfully', 'Close', { duration: 3000 });
    } catch (error) {
      this.snackBar.open(error.message || 'An error occurred during comparison', 'Close', {
        duration: 5000,
        panelClass: ['error-snackbar']
      });
    } finally {
      this.isLoading = false;
    }
  }

  async generateReport(format: 'excel' | 'csv' = 'excel'): Promise<void> {
    if (!this.canCompare || !this.comparisonResults) return;

    try {
      const blob = await this.comparisonService.generateReport(
        this.sourceConfig!,
        this.targetConfig!,
        [],  // We'll need to pass the actual table mappings here
        this.comparisonConfig!,
        format
      ).toPromise();

      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `comparison-report-${new Date().getTime()}.${format}`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);

      this.snackBar.open('Report generated successfully', 'Close', { duration: 3000 });
    } catch (error) {
      this.snackBar.open('Failed to generate report', 'Close', {
        duration: 5000,
        panelClass: ['error-snackbar']
      });
    }
  }
} 