// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { HistoryEntry as HistoryEntryType } from '../../types';

interface HistoryEntryProps {
  entry: HistoryEntryType;
  on_click: (query: string) => void;
}

function format_timestamp(ts: number): string {
  const d = new Date(ts);
  const hh = String(d.getHours()).padStart(2, '0');
  const mm = String(d.getMinutes()).padStart(2, '0');
  const ss = String(d.getSeconds()).padStart(2, '0');
  return `${hh}:${mm}:${ss}`;
}

export function HistoryEntryRow({ entry, on_click }: HistoryEntryProps) {
  return (
    <div className="rdb-history__entry" onClick={() => on_click(entry.query)}>
      <div className="rdb-history__entry-meta">
        <span>{format_timestamp(entry.timestamp)}</span>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
          <span>{entry.execution_time}ms</span>
          <span
            className={`rdb-history__entry-indicator ${
              entry.success
                ? 'rdb-history__entry-indicator--success'
                : 'rdb-history__entry-indicator--error'
            }`}
          />
        </div>
      </div>
      <div className="rdb-history__entry-query">{entry.query}</div>
    </div>
  );
}
