// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

import type { HistoryStorage } from '../types';

const DEFAULT_STORAGE_KEY = 'reifydb-shell-history';
const MAX_HISTORY = 1000;

/**
 * Default localStorage-based history storage
 */
export class LocalStorageHistoryStorage implements HistoryStorage {
  private key: string;

  constructor(key: string = DEFAULT_STORAGE_KEY) {
    this.key = key;
  }

  load(): string[] {
    try {
      const stored = localStorage.getItem(this.key);
      if (stored) {
        const parsed = JSON.parse(stored);
        if (Array.isArray(parsed)) {
          return parsed.filter((e): e is string => typeof e === 'string');
        }
      }
    } catch {
      // Ignore localStorage errors
    }
    return [];
  }

  save(entries: string[]): void {
    try {
      localStorage.setItem(this.key, JSON.stringify(entries));
    } catch {
      // Ignore localStorage errors (quota exceeded, etc.)
    }
  }
}

/**
 * In-memory history storage (no persistence)
 */
export class MemoryHistoryStorage implements HistoryStorage {
  private entries: string[] = [];

  load(): string[] {
    return [...this.entries];
  }

  save(entries: string[]): void {
    this.entries = [...entries];
  }
}

export class CommandHistory {
  private entries: string[] = [];
  private position: number = -1;
  private savedInput: string = '';
  private storage: HistoryStorage;

  constructor(storage?: HistoryStorage, historyKey?: string) {
    this.storage = storage ?? new LocalStorageHistoryStorage(historyKey);
    this.entries = this.storage.load();
  }

  add(command: string): void {
    const trimmed = command.trim();
    if (!trimmed) return;

    // Don't add duplicates at the end
    if (this.entries.length > 0 && this.entries[this.entries.length - 1] === trimmed) {
      this.reset();
      return;
    }

    this.entries.push(trimmed);

    // Limit size
    if (this.entries.length > MAX_HISTORY) {
      this.entries = this.entries.slice(-MAX_HISTORY);
    }

    this.storage.save(this.entries);
    this.reset();
  }

  previous(currentInput: string): string | null {
    if (this.entries.length === 0) return null;

    // Save current input when starting navigation
    if (this.position === -1) {
      this.savedInput = currentInput;
      this.position = this.entries.length;
    }

    if (this.position > 0) {
      this.position--;
      return this.entries[this.position];
    }

    return null;
  }

  next(): string | null {
    if (this.position === -1) return null;

    if (this.position < this.entries.length - 1) {
      this.position++;
      return this.entries[this.position];
    }

    if (this.position === this.entries.length - 1) {
      this.position = -1;
      return this.savedInput;
    }

    return null;
  }

  reset(): void {
    this.position = -1;
    this.savedInput = '';
  }

  getAll(): string[] {
    return [...this.entries];
  }

  clear(): void {
    this.entries = [];
    this.position = -1;
    this.savedInput = '';
    this.storage.save(this.entries);
  }
}
