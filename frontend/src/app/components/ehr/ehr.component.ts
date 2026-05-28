import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Subject } from 'rxjs';
import { debounceTime, takeUntil } from 'rxjs/operators';
import { ApiService } from '../../services/api.service';

interface EhrStatGroup { Pending: number; Pushed: number; Failed: number; total: number; }
interface EhrStats     { checkin: EhrStatGroup; checkout: EhrStatGroup; }
interface Toast        { id: number; message: string; type: 'success' | 'error' | 'info'; }

@Component({
  selector: 'app-ehr',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './ehr.component.html',
  styleUrl: './ehr.component.scss',
})
export class EhrComponent implements OnInit, OnDestroy {
  records: any[] = [];
  stats: EhrStats = {
    checkin:  { Pending: 0, Pushed: 0, Failed: 0, total: 0 },
    checkout: { Pending: 0, Pushed: 0, Failed: 0, total: 0 },
  };
  total       = 0;
  totalPages  = 1;
  currentPage = 1;
  pageLimit   = 50;
  loading     = false;

  searchInput     = '';
  punchTypeFilter = '';
  statusFilter    = '';
  dateFrom        = '';
  dateTo          = '';

  triggering: Record<string, boolean> = {};
  pushingIds  = new Set<number>();
  toasts: Toast[] = [];
  private toastId = 0;
  private pollTimer?: ReturnType<typeof setTimeout>;
  private searchSubject = new Subject<string>();
  private destroy$      = new Subject<void>();

  constructor(private api: ApiService) {}

  ngOnInit() {
    this.loadPage(1);
    this.searchSubject.pipe(debounceTime(400), takeUntil(this.destroy$)).subscribe(() => {
      this.currentPage = 1;
      this.loadPage(1);
    });
  }

  ngOnDestroy() {
    clearTimeout(this.pollTimer);
    this.destroy$.next();
    this.destroy$.complete();
  }

  loadPage(page: number) {
    this.currentPage = page;
    this.loading     = true;
    clearTimeout(this.pollTimer);

    const params: Record<string, any> = { page: this.currentPage, limit: this.pageLimit };
    if (this.searchInput)     params['search']     = this.searchInput;
    if (this.punchTypeFilter) params['punchType']  = this.punchTypeFilter;
    if (this.statusFilter)    params['pushStatus'] = this.statusFilter;
    if (this.dateFrom)        params['dateFrom']   = this.dateFrom;
    if (this.dateTo)          params['dateTo']     = this.dateTo;

    this.api.getEhrLogs(params).subscribe({
      next: (data) => {
        if (!data.success) { this.showToast('Error: ' + data.error, 'error'); return; }
        this.records    = data.data;
        this.total      = data.total;
        this.totalPages = data.totalPages;
        this.stats      = data.stats ?? this.stats;
        this.pollTimer  = setTimeout(() => this.loadPage(this.currentPage), 15000);
      },
      error: (err) => this.showToast('Network error: ' + err.message, 'error'),
      complete: () => { this.loading = false; },
    });
  }

  onSearchInput()     { this.searchSubject.next(this.searchInput); }
  onFilterChange()    { this.currentPage = 1; this.loadPage(1); }
  onPageLimitChange() { this.loadPage(1); }

  clearDates() {
    this.dateFrom = '';
    this.dateTo   = '';
    this.currentPage = 1;
    this.loadPage(1);
  }

  trigger(action: string, label: string) {
    if (this.triggering[action]) return;
    this.triggering[action] = true;
    this.showToast(`${label} triggered…`, 'info');
    this.api.triggerEhrAction(action).subscribe({
      next: (data) => {
        this.triggering[action] = false;
        this.showToast(data.message || `${label} started`, 'success');
        setTimeout(() => this.loadPage(this.currentPage), 2000);
      },
      error: (err) => {
        this.triggering[action] = false;
        this.showToast(`${label} failed: ${err.message}`, 'error');
      },
    });
  }

  pushSingle(row: any) {
    if (this.pushingIds.has(row.Id)) return;
    this.pushingIds.add(row.Id);
    this.showToast(`Pushing record ${row.Id} (${row.EmployeeId})…`, 'info');

    this.api.pushEhrSingle(row.Id).subscribe({
      next: (data) => {
        this.pushingIds.delete(row.Id);
        if (data.success) {
          this.showToast(`Record ${row.Id} pushed successfully`, 'success');
          row.PushStatus = 'Pushed';
        } else {
          this.showToast(`Record ${row.Id} failed: ${data.error}`, 'error');
          row.PushStatus = 'Failed';
        }
      },
      error: (err) => {
        this.pushingIds.delete(row.Id);
        row.PushStatus = 'Failed';
        this.showToast(`Push failed: ${err.error?.error ?? err.message}`, 'error');
      },
    });
  }

  isPushing(id: number) { return this.pushingIds.has(id); }

  punchLabel(type: string): string {
    return type === 'I' ? 'Check-In' : type === 'O' ? 'Check-Out' : type;
  }

  fmtDate(dt: string): string {
    if (!dt) return '—';
    return new Date(dt).toLocaleString('en-IN', {
      day: '2-digit', month: 'short', year: 'numeric',
      hour: '2-digit', minute: '2-digit',
    });
  }

  goToPage(page: number) {
    if (page < 1 || page > this.totalPages) return;
    this.loadPage(page);
  }

  getPageNumbers(): (number | string)[] {
    const delta = 2;
    const pages: number[] = [];
    for (let i = Math.max(1, this.currentPage - delta); i <= Math.min(this.totalPages, this.currentPage + delta); i++)
      pages.push(i);
    if (pages[0] > 1) pages.unshift(1);
    if (pages[pages.length - 1] < this.totalPages) pages.push(this.totalPages);

    const result: (number | string)[] = [];
    let prev = 0;
    for (const p of pages) {
      if (prev && p - prev > 1) result.push('…');
      result.push(p);
      prev = p;
    }
    return result;
  }

  showToast(message: string, type: 'success' | 'error' | 'info' = 'info') {
    const id = ++this.toastId;
    this.toasts.push({ id, message, type });
    setTimeout(() => { this.toasts = this.toasts.filter(t => t.id !== id); }, 4000);
  }
}
