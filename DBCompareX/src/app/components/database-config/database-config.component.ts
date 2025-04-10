import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { MatSnackBar } from '@angular/material/snack-bar';
import { DatabaseConfig } from '../../models/database-config.interface';
import { DatabaseService } from '../../services/database.service';

@Component({
  selector: 'app-database-config',
  templateUrl: './database-config.component.html',
  styleUrls: ['./database-config.component.scss']
})
export class DatabaseConfigComponent implements OnInit {
  @Input() configType: 'source' | 'target' = 'source';
  @Output() configChange = new EventEmitter<DatabaseConfig>();

  configForm: FormGroup;
  hidePassword = true;
  isLoading = false;
  connectionStatus: 'none' | 'success' | 'error' = 'none';

  constructor(
    private fb: FormBuilder,
    private databaseService: DatabaseService,
    private snackBar: MatSnackBar
  ) {
    this.configForm = this.fb.group({
      dbType: ['mysql', Validators.required],
      host: ['localhost', Validators.required],
      port: [3306, [Validators.required, Validators.min(1), Validators.max(65535)]],
      schema: ['', Validators.required],
      username: ['', Validators.required],
      password: ['', Validators.required]
    });
  }

  ngOnInit(): void {
    // Load saved configuration if available
    const savedConfig = localStorage.getItem(`${this.configType}Config`);
    if (savedConfig) {
      const config = JSON.parse(savedConfig);
      this.configForm.patchValue(config);
    }

    // Update form based on database type
    this.configForm.get('dbType')?.valueChanges.subscribe(dbType => {
      const portControl = this.configForm.get('port');
      switch (dbType) {
        case 'mysql':
          portControl?.setValue(3306);
          break;
        case 'postgresql':
          portControl?.setValue(5432);
          break;
        case 'oracle':
          portControl?.setValue(1521);
          break;
        case 'sqlserver':
          portControl?.setValue(1433);
          break;
      }
    });

    // Emit changes when form is valid
    this.configForm.valueChanges.subscribe(() => {
      if (this.configForm.valid) {
        this.emitConfig();
      }
    });
  }

  async testConnection(): Promise<void> {
    if (this.configForm.invalid) return;

    this.isLoading = true;
    this.connectionStatus = 'none';

    try {
      const config = this.configForm.value;
      const success = await this.databaseService.testConnection(config).toPromise();
      
      this.connectionStatus = success ? 'success' : 'error';
      this.snackBar.open(
        success ? 'Connection successful!' : 'Connection failed!',
        'Close',
        {
          duration: 3000,
          panelClass: success ? ['success-snackbar'] : ['error-snackbar']
        }
      );
    } catch (error) {
      this.connectionStatus = 'error';
      this.snackBar.open(
        'Connection failed: ' + (error.message || 'Unknown error'),
        'Close',
        {
          duration: 5000,
          panelClass: ['error-snackbar']
        }
      );
    } finally {
      this.isLoading = false;
    }
  }

  onSubmit(): void {
    if (this.configForm.valid) {
      this.emitConfig();
    } else {
      this.markFormGroupTouched(this.configForm);
    }
  }

  private emitConfig(): void {
    const config = this.configForm.value;
    localStorage.setItem(`${this.configType}Config`, JSON.stringify(config));
    this.configChange.emit(config);
  }

  private markFormGroupTouched(formGroup: FormGroup): void {
    Object.values(formGroup.controls).forEach(control => {
      control.markAsTouched();
      if (control instanceof FormGroup) {
        this.markFormGroupTouched(control);
      }
    });
  }
} 