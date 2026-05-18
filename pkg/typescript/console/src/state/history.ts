// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { HistoryEntry } from '../types';

const DEFAULT_KEY = 'reifydb-console-history';
const MAX_ENTRIES = 500;

export function load_history(key: string = DEFAULT_KEY): HistoryEntry[] {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return [];
    const entries = JSON.parse(raw);
    return Array.isArray(entries) ? entries : [];
  } catch {
    return [];
  }
}

export function save_history(entries: HistoryEntry[], key: string = DEFAULT_KEY): void {
  try {
    const trimmed = entries.slice(0, MAX_ENTRIES);
    localStorage.setItem(key, JSON.stringify(trimmed));
  } catch {
    // localStorage may be unavailable
  }
}

export function clear_history(key: string = DEFAULT_KEY): void {
  try {
    localStorage.removeItem(key);
  } catch {
    // localStorage may be unavailable
  }
}
