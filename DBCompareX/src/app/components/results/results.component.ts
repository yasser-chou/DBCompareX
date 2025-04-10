import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-results',
  templateUrl: './results.component.html',
  styleUrls: ['./results.component.scss']
})
export class ResultsComponent implements OnInit {
  @Input() comparisonResults: any;

  constructor() { }

  ngOnInit(): void {
  }

  downloadReport(): void {
    // Implementation will be added later
    console.log('Downloading report...');
  }
} 