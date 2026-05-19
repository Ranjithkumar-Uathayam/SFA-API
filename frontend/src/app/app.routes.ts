import { Routes } from '@angular/router';

export const routes: Routes = [
  {
    path: '',
    loadComponent: () => import('./components/dashboard/dashboard.component').then(m => m.DashboardComponent),
  },
  {
    path: 'products',
    loadComponent: () => import('./components/products/products.component').then(m => m.ProductsComponent),
  },
  {
    path: 'master/:type',
    loadComponent: () => import('./components/master/master.component').then(m => m.MasterComponent),
  },
  { path: '**', redirectTo: '' },
];
