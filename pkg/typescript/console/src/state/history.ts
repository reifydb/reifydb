// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

import type { HistoryEntry } from '../types';

const DEFAULT_KEY = 'reifydb-console-history';
const MAX_ENTRIES = 500;

export function loadHistory(key: string = DEFAULT_KEY): HistoryEntry[] {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return [];
    const entries = JSON.parse(raw);
    return Array.isArray(entries) ? entries : [];
  } catch {
    return [];
  }
}

export function saveHistory(entries: HistoryEntry[], key: string = DEFAULT_KEY): void {
  try {
    const trimmed = entries.slice(0, MAX_ENTRIES);
    localStorage.setItem(key, JSON.stringify(trimmed));
  } catch {
    // localStorage may be unavailable
  }
}

export function clearHistory(key: string = DEFAULT_KEY): void {
  try {
    localStorage.removeItem(key);
  } catch {
    // localStorage may be unavailable
  }
}
