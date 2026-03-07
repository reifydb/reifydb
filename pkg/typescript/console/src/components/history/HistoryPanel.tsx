// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useState } from 'react';
import type { HistoryEntry } from '../../types';
import { HistoryEntryRow } from './HistoryEntry';

interface HistoryPanelProps {
  entries: HistoryEntry[];
  onSelect: (query: string) => void;
}

export function HistoryPanel({ entries, onSelect }: HistoryPanelProps) {
  const [search, setSearch] = useState('');

  const filtered = search
    ? entries.filter(e => e.query.toLowerCase().includes(search.toLowerCase()))
    : entries;

  return (
    <div className="rdb-history">
      <div className="rdb-history__search">
        <input
          type="text"
          className="rdb-history__search-input"
          placeholder="search history..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
      </div>
      <div className="rdb-history__list">
        {filtered.length === 0 ? (
          <div className="rdb-history__empty">
            {entries.length === 0 ? '$ no history yet' : '$ no matching queries'}
          </div>
        ) : (
          filtered.map((entry) => (
            <HistoryEntryRow key={entry.id} entry={entry} onClick={onSelect} />
          ))
        )}
      </div>
    </div>
  );
}
