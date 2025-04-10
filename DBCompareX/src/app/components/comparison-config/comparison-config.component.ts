import { Component, OnInit } from '@angular/core';
import { FormBuilder, FormGroup, FormArray, Validators } from '@angular/forms';
import { MatSnackBar } from '@angular/material/snack-bar';

interface TableMapping {
  sourceColumn: string;
  targetColumn: string;
  transformationType: string;
}

@Component({
  selector: 'app-comparison-config',
  templateUrl: './comparison-config.component.html',
  styleUrls: ['./comparison-config.component.scss']
})
export class ComparisonConfigComponent implements OnInit {
  configForm: FormGroup;

  constructor(
    private fb: FormBuilder,
    private snackBar: MatSnackBar
  ) {
    this.configForm = this.fb.group({
      configName: ['', Validators.required],
      caseSensitive: [false],
      ignoreWhitespace: [true],
      ignoreNulls: [true],
      stringSimilarityThreshold: [0.8, [Validators.required, Validators.min(0), Validators.max(1)]],
      numericTolerance: [0.001, [Validators.required, Validators.min(0)]],
      tableMappings: this.fb.array([])
    });
  }

  ngOnInit(): void {
    // Add initial empty mapping
    this.addMapping();
  }

  get tableMappings(): FormArray {
    return this.configForm.get('tableMappings') as FormArray;
  }

  createMappingFormGroup(): FormGroup {
    return this.fb.group({
      sourceColumn: ['', Validators.required],
      targetColumn: ['', Validators.required],
      transformationType: ['none']
    });
  }

  addMapping(): void {
    this.tableMappings.push(this.createMappingFormGroup());
  }

  removeMapping(index: number): void {
    this.tableMappings.removeAt(index);
  }

  updateTransformation(index: number): void {
    const mapping = this.tableMappings.at(index);
    const transformationType = mapping.get('transformationType')?.value;
    
    // Additional logic for transformation updates can be added here
    console.log(`Updated transformation for mapping ${index} to ${transformationType}`);
  }

  onSubmit(): void {
    if (this.configForm.valid) {
      const config = this.configForm.value;
      console.log('Saving configuration:', config);
      
      // TODO: Add API call to save configuration
      
      this.snackBar.open('Configuration saved successfully', 'Close', {
        duration: 3000,
        horizontalPosition: 'right',
        verticalPosition: 'top'
      });
    } else {
      this.snackBar.open('Please fill in all required fields', 'Close', {
        duration: 3000,
        horizontalPosition: 'right',
        verticalPosition: 'top'
      });
      this.markFormGroupTouched(this.configForm);
    }
  }

  private markFormGroupTouched(formGroup: FormGroup | FormArray): void {
    Object.values(formGroup.controls).forEach(control => {
      if (control instanceof FormGroup || control instanceof FormArray) {
        this.markFormGroupTouched(control);
      } else {
        control.markAsTouched();
      }
    });
  }
} 