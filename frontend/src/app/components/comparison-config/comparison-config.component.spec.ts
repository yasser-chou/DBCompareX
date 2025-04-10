import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ComparisonConfigComponent } from './comparison-config.component';

describe('ComparisonConfigComponent', () => {
  let component: ComparisonConfigComponent;
  let fixture: ComponentFixture<ComparisonConfigComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ComparisonConfigComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(ComparisonConfigComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
