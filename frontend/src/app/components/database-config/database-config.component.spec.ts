import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DatabaseConfigComponent } from './database-config.component';

describe('DatabaseConfigComponent', () => {
  let component: DatabaseConfigComponent;
  let fixture: ComponentFixture<DatabaseConfigComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [DatabaseConfigComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(DatabaseConfigComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
