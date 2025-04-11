import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { MatSnackBar } from '@angular/material/snack-bar';
import { DatabaseService, DatabaseConfig } from '../../services/database.service';
import { timeout, catchError } from 'rxjs/operators';
import { of } from 'rxjs';

@Component({
  selector: 'app-database-connection-form',
  templateUrl: './database-connection-form.component.html',
  styleUrls: ['./database-connection-form.component.scss']
})
export class DatabaseConnectionFormComponent implements OnInit {
  @Input() type: 'source' | 'target' = 'source';
  @Input() initialConfig?: DatabaseConfig;

  @Output() connectionStatusChange = new EventEmitter<{success: boolean, message: string}>();
  @Output() tablesLoaded = new EventEmitter<string[]>();

  dbForm: FormGroup;
  tables: string[] = [];
  isLoading = false;
  connectionStatus: {success: boolean, message: string} | null = null;

  constructor(
    private fb: FormBuilder,
    private dbService: DatabaseService,
    private snackBar: MatSnackBar
  ) {
    this.dbForm = this.createDbForm();
  }

  ngOnInit(): void {
    // Apply initial config if provided
    if (this.initialConfig) {
      this.dbForm.patchValue(this.initialConfig);
    }

    // Add listeners for dbType changes to set default ports
    this.dbForm.get('dbType')?.valueChanges.subscribe(dbType => {
      this.setDefaultPort(dbType);
      this.updateValidators(dbType);
    });

    // Set initial validators based on current value
    this.updateValidators(this.dbForm.get('dbType')?.value);
  }

  createDbForm(): FormGroup {
    return this.fb.group({
      dbType: ['', Validators.required],
      host: ['', Validators.required],
      port: ['', [Validators.required, Validators.pattern('^[0-9]*$')]],
      username: ['', Validators.required],
      password: ['', Validators.required],
      schema: ['', Validators.required],
      databaseName: [''],
      table: ['']
    });
  }

  handleEnterKey(): void {
    // Only connect if the form is valid and we're not already loading
    if (this.dbForm.valid && !this.isLoading) {
      this.connect();
    }
  }

  connect(): void {
    if (this.dbForm.valid) {
      // Clear any previous connection status
      this.connectionStatus = null;
      
      // Check for common Oracle port mistakes
      if (this.dbForm.value.dbType === 'oracle' && this.dbForm.value.port !== '1521') {
        this.snackBar.open('Warning: The standard Oracle port is 1521. Please verify your port number.', 'Close', {
          duration: 5000,
          panelClass: ['error-snackbar', 'custom-snackbar'],
          horizontalPosition: 'right',
          verticalPosition: 'top'
        });
      }

      // Check for missing schema filter for Oracle
      if (this.dbForm.value.dbType === 'oracle' && !this.dbForm.value.databaseName) {
        this.snackBar.open('Error: Schema Filter is required for Oracle connections to limit table fetching.', 'Close', {
          duration: 5000,
          panelClass: ['error-snackbar', 'custom-snackbar'],
          horizontalPosition: 'right',
          verticalPosition: 'top'
        });
        return;
      }

      this.isLoading = true;
      const config = this.dbForm.value as DatabaseConfig;

      // Set timeout for connection
      this.dbService.testConnection(config, this.type)
        .pipe(
          timeout(30000), // 30 second timeout
          catchError(error => {
            this.isLoading = false;
            const status = {
              success: false,
              message: error.name === 'TimeoutError'
                ? 'Connection timed out. Please check your settings or try again later.'
                : (error.message || 'Connection failed')
            };
            this.connectionStatus = status;
            this.connectionStatusChange.emit(status);
            this.snackBar.open(status.message, 'Close', {
              duration: 5000,
              panelClass: ['error-snackbar', 'custom-snackbar'],
              horizontalPosition: 'right',
              verticalPosition: 'top'
            });
            return of(false);
          })
        )
        .subscribe({
          next: (success) => {
            const status = {
              success: success,
              message: success ? 'Connected successfully' : 'Connection failed'
            };
            this.connectionStatus = status;
            this.connectionStatusChange.emit(status);

            if (success) {
              // Load tables only if connection was successful
              this.dbService.getTables(config)
                .pipe(
                  timeout(30000), // 30 second timeout
                  catchError(error => {
                    this.isLoading = false;
                    const tableStatus = {
                      success: false,
                      message: error.name === 'TimeoutError'
                        ? 'Loading tables timed out. The database might be too large.'
                        : (error.message || 'Failed to load tables')
                    };
                    this.connectionStatus = tableStatus;
                    this.connectionStatusChange.emit(tableStatus);
                    this.snackBar.open(tableStatus.message, 'Close', {
                      duration: 5000,
                      panelClass: ['error-snackbar', 'custom-snackbar'],
                      horizontalPosition: 'right',
                      verticalPosition: 'top'
                    });
                    return of([]);
                  })
                )
                .subscribe({
                  next: (tables) => {
                    this.tables = tables;
                    this.isLoading = false;
                    this.snackBar.open(`Tables fetched successfully from ${this.type} database`, 'Close', {
                      duration: 5000,
                      panelClass: ['success-snackbar', 'custom-snackbar'],
                      horizontalPosition: 'right',
                      verticalPosition: 'top'
                    });
                    this.tablesLoaded.emit(tables);
                  }
                });
            } else {
              this.isLoading = false;
              const failStatus = {
                success: false,
                message: `Connection to ${this.type} database failed`
              };
              this.connectionStatus = failStatus;
              this.connectionStatusChange.emit(failStatus);
              this.snackBar.open(`Connection to ${this.type} database failed`, 'Close', {
                duration: 5000,
                panelClass: ['error-snackbar', 'custom-snackbar'],
                horizontalPosition: 'right',
                verticalPosition: 'top'
              });
            }
          }
        });
    }
  }

  setDefaultPort(dbType: string): void {
    if (!dbType) return;

    const portControl = this.dbForm.get('port');
    if (portControl && (portControl.value === '' || portControl.pristine)) {
      switch (dbType.toLowerCase()) {
        case 'mysql':
          portControl.setValue('3306');
          break;
        case 'postgres':
          portControl.setValue('5432');
          break;
        case 'sqlserver':
          portControl.setValue('1433');
          break;
        case 'oracle':
          portControl.setValue('1521');
          break;
      }
    }
  }

  updateValidators(dbType: string): void {
    if (!dbType) return;

    const databaseNameControl = this.dbForm.get('databaseName');
    if (databaseNameControl) {
      if (dbType.toLowerCase() === 'oracle') {
        databaseNameControl.setValidators([Validators.required]);
      } else {
        databaseNameControl.clearValidators();
      }
      databaseNameControl.updateValueAndValidity();
    }
  }

  get dbType(): string {
    return this.dbForm.get('dbType')?.value || '';
  }

  getFormValue(): DatabaseConfig {
    return this.dbForm.value as DatabaseConfig;
  }
}
