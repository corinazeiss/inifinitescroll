import { Injectable, DestroyRef } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Observable, Subject } from 'rxjs';
import { Comment } from '../models/comment.model';
import { MockCommentService } from './mock-comment.service';

const LOOKAHEAD_BUFFER = 15;
const MEMORY_WINDOW_HALF = 60;

@Injectable({ providedIn: 'root' })
export class CommentDataProvider {
  // WS messages appended at the bottom — kept fully in memory, never evicted.
  private _newItems: Comment[] = [];

  // Sparse map of index → Comment for paged data. Index 0 = oldest item.
  // Only the scroll window is retained; items outside it are evicted and
  // restored from _pageCache when scrolled back into view.
  private _pagedData = new Map<number, Comment>();

  private _inMemoryCount = 0;

  private get _pagedCount(): number { return this.service.total - this._newItems.length; }

  private readonly _data$ = new Subject<(Comment | null)[]>();
  private readonly _loading$ = new Subject<boolean>();
  private readonly _inMemoryCount$ = new Subject<number>();

  private _isLoading = false;
  private readonly _pageCache = new Map<number, Comment[]>();
  private readonly _pendingPages = new Set<number>();

  // Pages are fetched newest-first. _nextOlderPage tracks which page to fetch
  // next as the user scrolls up toward older history; -1 means fully loaded.
  private _nextOlderPage: number;

  // Current window bounds kept up-to-date so async re-fetches know where to restore.
  private _windowStart = 0;
  private _windowEnd = 0;

  readonly comments$: Observable<(Comment | null)[]> = this._data$.asObservable();
  readonly loading$: Observable<boolean> = this._loading$.asObservable();
  readonly inMemoryCount$: Observable<number> = this._inMemoryCount$.asObservable();

  get totalFetched(): number {
    return this.service.total;
  }

  constructor(
    private readonly service: MockCommentService,
    private readonly destroyRef: DestroyRef,
  ) {
    this.destroyRef.onDestroy(() => {
      this._data$.complete();
      this._loading$.complete();
      this._inMemoryCount$.complete();
    });
    const totalPages = Math.ceil(this.service.total / this.service.pageSize);
    this._nextOlderPage = totalPages - 1;
    this._fetchOlderPage();
  }

  // Appends a WS message at the bottom of the chat.
  appendMessage(comment: Comment): void {
    this._newItems = [...this._newItems, comment];
    this.service.addItems(1);
    this._emitData();
  }

  onScrolled(firstVisible: number, _lastVisible: number): void {
    // Paged items occupy indices 0.._pagedCount-1; WS items follow after.
    // Eviction only applies to the paged section.
    const pagedFirst = Math.min(firstVisible, this._pagedCount - 1);

    this._evictAndRestore(pagedFirst);

    // Fetch older pages as user scrolls toward the top (index 0 = oldest).
    // earliestLoadedIndex is the first index of the oldest page already fetched;
    // trigger when the user gets within LOOKAHEAD_BUFFER of it.
    if (this._nextOlderPage >= 0) {
      const earliestLoadedIndex = (this._nextOlderPage + 1) * this.service.pageSize;
      if (pagedFirst <= earliestLoadedIndex + LOOKAHEAD_BUFFER) {
        this._fetchOlderPage();
      }
    }
  }

  private _evictAndRestore(firstVisible: number): void {
    const pageSize = this.service.pageSize;
    const windowStart = Math.max(0, firstVisible - MEMORY_WINDOW_HALF);
    const windowEnd = Math.min(this._pagedCount - 1, firstVisible + MEMORY_WINDOW_HALF);

    this._windowStart = windowStart;
    this._windowEnd = windowEnd;

    let changed = false;

    for (const i of this._pagedData.keys()) {
      if (i < windowStart || i > windowEnd) {
        this._pagedData.delete(i);
        changed = true;
      }
    }

    const startPage = Math.floor(windowStart / pageSize);
    const endPage = Math.floor(windowEnd / pageSize);

    for (const page of this._pageCache.keys()) {
      if (page < startPage || page > endPage) {
        this._pageCache.delete(page);
      }
    }

    for (let page = startPage; page <= endPage; page++) {
      const cached = this._pageCache.get(page);
      if (!cached) {
        this._refetchPage(page);
        continue;
      }

      const pageStart = page * pageSize;
      for (let i = 0; i < cached.length; i++) {
        const idx = pageStart + i;
        if (idx >= windowStart && idx <= windowEnd && idx < this._pagedCount && !this._pagedData.has(idx)) {
          this._pagedData.set(idx, cached[i]);
          changed = true;
        }
      }
    }

    if (changed) {
      this._emitData();
    }
  }

  private _refetchPage(page: number): void {
    if (this._pendingPages.has(page)) return;
    this._pendingPages.add(page);

    this.service
      .getPage(page)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this._pendingPages.delete(page);
          this._pageCache.set(page, response.items);

          const pageStart = page * this.service.pageSize;
          let changed = false;
          for (let i = 0; i < response.items.length; i++) {
            const idx = pageStart + i;
            if (idx >= this._windowStart && idx <= this._windowEnd && !this._pagedData.has(idx)) {
              this._pagedData.set(idx, response.items[i]);
              changed = true;
            }
          }
          if (changed) this._emitData();
        },
        error: () => this._pendingPages.delete(page),
      });
  }

  private _fetchOlderPage(): void {
    if (this._isLoading || this._nextOlderPage < 0) return;

    this._isLoading = true;
    this._loading$.next(true);
    const page = this._nextOlderPage;

    this.service
      .getPage(page)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this._pageCache.set(page, response.items);
          const pageStart = page * this.service.pageSize;
          response.items.forEach((item, i) => this._pagedData.set(pageStart + i, item));
          this._nextOlderPage--;
          this._isLoading = false;
          this._loading$.next(false);
          this._emitData();
        },
        error: () => {
          this._isLoading = false;
          this._loading$.next(false);
        },
      });
  }

  private _emitData(): void {
    const pagedCount = this._pagedCount;
    const totalLength = this.service.total;

    // Layout: paged history at 0..pagedCount-1, WS messages after.
    // Fill with explicit null for evicted slots so the array is dense — sparse
    // empty slots can confuse iterable diffing.
    const combined: (Comment | null)[] = new Array(totalLength).fill(null);
    for (const [i, comment] of this._pagedData) {
      combined[i] = comment;
    }
    for (let i = 0; i < this._newItems.length; i++) {
      combined[pagedCount + i] = this._newItems[i];
    }

    // Find the highest index that's been loaded. This is crucial for preventing
    // empty space in the virtual scroll viewport when starting with only recent pages.
    let maxLoadedIndex = -1;
    for (const idx of this._pagedData.keys()) {
      if (idx > maxLoadedIndex) maxLoadedIndex = idx;
    }
    if (this._newItems.length > 0) {
      maxLoadedIndex = Math.max(maxLoadedIndex, pagedCount + this._newItems.length - 1);
    }

    // Trim the array to avoid rendering placeholder space for unloaded items.
    // The indices of loaded items are preserved; we just don't allocate space for future loads.
    const result = maxLoadedIndex >= 0 ? combined.slice(0, maxLoadedIndex + 1) : combined;

    this._inMemoryCount = this._newItems.length + this._pagedData.size;
    this._data$.next(result);
    this._inMemoryCount$.next(this._inMemoryCount);
  }
}
