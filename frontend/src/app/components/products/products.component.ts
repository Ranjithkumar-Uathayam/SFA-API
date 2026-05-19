import { Component, OnInit, OnDestroy, ViewChild, ElementRef, AfterViewChecked } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Subject } from 'rxjs';
import { debounceTime, takeUntil } from 'rxjs/operators';
import { ApiService } from '../../services/api.service';

interface Product {
  ProductCode: string;
  ProductName: string;
  Brand?: string;
  DivisionCode?: string;
  CategoryName?: string;
  StyleCode?: string;
  SizeCode?: string;
  ColorCode?: string;
  ColorName?: string;
  UOM?: string;
  HSNCode?: string;
  ProductIsActive?: boolean;
  PushStatus?: string;
  LastPushedAt?: string;
  PushError?: string;
}

interface Summary { Pending?: number; Pushed?: number; Failed?: number; Pushing?: number; }

export interface Toast { id: number; message: string; type: 'success' | 'error' | 'info'; }

interface PushResult {
  success: boolean;
  totalRequested?: number;
  totalMapped?: number;
  successCount?: number;
  failedCount?: number;
  failedProducts?: Array<{ code: string; error: any }>;
}

@Component({
  selector: 'app-products',
  imports: [CommonModule, FormsModule],
  templateUrl: './products.component.html',
  styleUrl: './products.component.scss',
})
export class ProductsComponent implements OnInit, OnDestroy, AfterViewChecked {
  @ViewChild('selectAllRef') selectAllRef?: ElementRef<HTMLInputElement>;

  products: Product[] = [];
  summary: Summary = {};
  totalRecords = 0;
  totalPages = 1;
  currentPage = 1;
  pageSize = 50;
  loading = false;

  searchInput = '';
  pushStatusFilter = '';
  divisionFilter = '';

  selectedCodes = new Set<string>();
  pushingCodes = new Set<string>();

  lastRefreshed = '';
  resultPanel: PushResult | null = null;
  showResultPanel = false;
  toasts: Toast[] = [];
  private toastId = 0;

  private pollTimer?: ReturnType<typeof setTimeout>;
  private searchSubject = new Subject<string>();
  private destroy$ = new Subject<void>();
  private needSelectAllSync = false;

  constructor(private api: ApiService) {}

  ngOnInit() {
    this.searchSubject
      .pipe(debounceTime(400), takeUntil(this.destroy$))
      .subscribe(val => {
        this.currentPage = 1;
        this.loadProducts(true);
      });
    this.loadProducts(true);
  }

  ngAfterViewChecked() {
    if (this.needSelectAllSync && this.selectAllRef) {
      const el = this.selectAllRef.nativeElement;
      const nonPushing = this.products.filter(p => !this.pushingCodes.has(p.ProductCode));
      const checkedCount = nonPushing.filter(p => this.selectedCodes.has(p.ProductCode)).length;
      el.checked = nonPushing.length > 0 && checkedCount === nonPushing.length;
      el.indeterminate = checkedCount > 0 && checkedCount < nonPushing.length;
      this.needSelectAllSync = false;
    }
  }

  ngOnDestroy() {
    clearTimeout(this.pollTimer);
    this.destroy$.next();
    this.destroy$.complete();
  }

  loadProducts(showBar = false) {
    if (showBar) this.loading = true;
    clearTimeout(this.pollTimer);

    const params: Record<string, any> = { page: this.currentPage, limit: this.pageSize };
    if (this.searchInput)      params['search']     = this.searchInput;
    if (this.pushStatusFilter) params['pushStatus'] = this.pushStatusFilter;
    if (this.divisionFilter)   params['division']   = this.divisionFilter;

    this.api.getProducts(params).subscribe({
      next: (json) => {
        if (!json.success) { this.showToast('Load failed: ' + json.error, 'error'); return; }
        this.products     = json.data;
        this.totalPages   = json.totalPages;
        this.totalRecords = json.total;
        this.summary      = json.summary || {};
        this.lastRefreshed = new Date().toLocaleTimeString();
        this.needSelectAllSync = true;

        const hasPushing = json.data.some((r: Product) => r.PushStatus === 'Pushing') || this.pushingCodes.size > 0;
        if (hasPushing) {
          this.pollTimer = setTimeout(() => this.loadProducts(false), 3500);
        }
      },
      error: (err) => this.showToast('Error loading products: ' + err.message, 'error'),
      complete: () => { this.loading = false; },
    });
  }

  onSearchInput() {
    this.searchSubject.next(this.searchInput);
  }

  clearSearch() {
    this.searchInput = '';
    this.currentPage = 1;
    this.loadProducts(true);
  }

  filterByStatus(status: string) {
    this.pushStatusFilter = status;
    this.currentPage = 1;
    this.loadProducts(true);
  }

  onDivisionChange() {
    this.currentPage = 1;
    this.loadProducts(true);
  }

  onPageSizeChange() {
    this.currentPage = 1;
    this.loadProducts(true);
  }

  goToPage(page: number) {
    if (page < 1 || page > this.totalPages) return;
    this.currentPage = page;
    this.selectedCodes.clear();
    this.needSelectAllSync = true;
    this.loadProducts(true);
  }

  isRowPushing(code: string) { return this.pushingCodes.has(code); }
  isRowSelected(code: string) { return this.selectedCodes.has(code); }

  toggleRow(code: string, checked: boolean) {
    if (checked) this.selectedCodes.add(code);
    else         this.selectedCodes.delete(code);
    this.needSelectAllSync = true;
  }

  toggleSelectAll(checked: boolean) {
    this.products.forEach(p => {
      if (!this.pushingCodes.has(p.ProductCode)) {
        if (checked) this.selectedCodes.add(p.ProductCode);
        else         this.selectedCodes.delete(p.ProductCode);
      }
    });
    this.needSelectAllSync = true;
  }

  clearSelection() {
    this.selectedCodes.clear();
    this.needSelectAllSync = true;
  }

  get selectionCount() { return this.selectedCodes.size; }

  pushSingle(code: string) { this.doPush([code]); }

  pushSelected() {
    const codes = [...this.selectedCodes];
    if (!codes.length) { this.showToast('No products selected.', 'info'); return; }
    this.clearSelection();
    this.doPush(codes);
  }

  doPush(codes: string[]) {
    codes.forEach(c => this.pushingCodes.add(c));
    this.showToast(`Pushing ${codes.length} product(s)…`, 'info');

    this.api.pushProducts(codes).subscribe({
      next: (result) => {
        codes.forEach(c => this.pushingCodes.delete(c));
        this.resultPanel = result;
        this.showResultPanel = true;
        const s = result.successCount ?? 0;
        const f = result.failedCount  ?? 0;
        this.showToast(`Push done — ${s} pushed, ${f} failed.`, result.success ? 'success' : 'error');
        this.loadProducts(false);
      },
      error: (err) => {
        codes.forEach(c => this.pushingCodes.delete(c));
        this.showToast('Push error: ' + err.message, 'error');
        this.loadProducts(false);
      },
    });
  }

  triggerPushAll() {
    if (!confirm('Push ALL products matching the current filter?\n\nThis runs in the background.')) return;
    const filters: Record<string, any> = {};
    if (this.searchInput)      filters['search']     = this.searchInput;
    if (this.pushStatusFilter) filters['pushStatus'] = this.pushStatusFilter;
    if (this.divisionFilter)   filters['division']   = this.divisionFilter;

    this.loading = true;
    this.api.pushAllProducts(filters).subscribe({
      next: (json) => {
        if (json.success) {
          this.showToast('Push-All started in background. Statuses will update automatically.', 'info');
          this.pollTimer = setTimeout(() => this.loadProducts(false), 4000);
        } else {
          this.showToast('Push-All error: ' + json.error, 'error');
        }
      },
      error: (err) => this.showToast('Push-All error: ' + err.message, 'error'),
      complete: () => { this.loading = false; },
    });
  }

  closeResultPanel() { this.showResultPanel = false; }

  getPageNumbers(): (number | string)[] {
    const pages: (number | string)[] = [];
    if (this.totalPages <= 7) {
      for (let i = 1; i <= this.totalPages; i++) pages.push(i);
    } else {
      pages.push(1);
      if (this.currentPage > 3) pages.push('…');
      for (let p = Math.max(2, this.currentPage - 1); p <= Math.min(this.totalPages - 1, this.currentPage + 1); p++) pages.push(p);
      if (this.currentPage < this.totalPages - 2) pages.push('…');
      pages.push(this.totalPages);
    }
    return pages;
  }

  getPageInfo(): string {
    if (this.totalRecords === 0) return 'No records';
    const from = (this.currentPage - 1) * this.pageSize + 1;
    const to   = Math.min(this.currentPage * this.pageSize, this.totalRecords);
    return `Showing ${from}–${to} of ${this.totalRecords.toLocaleString()} products`;
  }

  rowIndex(i: number) { return (this.currentPage - 1) * this.pageSize + i + 1; }

  fmtDate(dt: string): string {
    const d = new Date(dt);
    return d.toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: 'numeric' })
      + ' ' + d.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' });
  }

  truncate(str: string | undefined | null, len: number): string {
    if (!str) return '—';
    return str.length > len ? str.slice(0, len) + '…' : str;
  }

  errStr(e: any): string {
    return typeof e === 'string' ? e : JSON.stringify(e);
  }

  showToast(message: string, type: 'success' | 'error' | 'info' = 'info') {
    const id = ++this.toastId;
    this.toasts.push({ id, message, type });
    setTimeout(() => { this.toasts = this.toasts.filter(t => t.id !== id); }, 5000);
  }
}
