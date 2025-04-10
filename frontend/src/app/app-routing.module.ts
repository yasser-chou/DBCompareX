import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { DatabaseConfigComponent } from './components/database-config/database-config.component';
import { ComparisonConfigComponent } from './components/comparison-config/comparison-config.component';
import { ResultsComponent } from './components/results/results.component';

const routes: Routes = [
  { path: '', redirectTo: '/database-config', pathMatch: 'full' },
  { path: 'database-config', component: DatabaseConfigComponent },
  { path: 'comparison-config', component: ComparisonConfigComponent },
  { path: 'results', component: ResultsComponent },
  { path: '**', redirectTo: '/database-config' }
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
