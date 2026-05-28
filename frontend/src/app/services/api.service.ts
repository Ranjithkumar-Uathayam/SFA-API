import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class ApiService {
  constructor(private http: HttpClient) {}

  getProducts(params: Record<string, any>): Observable<any> {
    return this.http.get('/api/products', { params: this.toHttpParams(params) });
  }

  pushProducts(productCodes: string[]): Observable<any> {
    return this.http.post('/api/products/push', { productCodes });
  }

  pushAllProducts(filters: Record<string, any>): Observable<any> {
    return this.http.post('/api/products/push-all', filters);
  }

  getMasterList(type: string, params: Record<string, any>): Observable<any> {
    return this.http.get(`/api/master/${type}/list`, { params: this.toHttpParams(params) });
  }

  pushMaster(type: string, recordKeys: string[]): Observable<any> {
    return this.http.post(`/api/master/${type}/push`, { recordKeys });
  }

  pushAllMaster(type: string, filters: Record<string, any>): Observable<any> {
    return this.http.post(`/api/master/${type}/push-all`, filters);
  }

  getEhrLogs(params: Record<string, any>): Observable<any> {
    return this.http.get('/api/ehr/logs', { params: this.toHttpParams(params) });
  }

  triggerEhrAction(action: string): Observable<any> {
    return this.http.post(`/api/ehr/trigger/${action}`, {});
  }

  pushEhrSingle(id: number): Observable<any> {
    return this.http.post(`/api/ehr/push/${id}`, {});
  }

  private toHttpParams(obj: Record<string, any>): HttpParams {
    let params = new HttpParams();
    for (const [k, v] of Object.entries(obj)) {
      if (v !== undefined && v !== null && v !== '') {
        params = params.set(k, String(v));
      }
    }
    return params;
  }
}
