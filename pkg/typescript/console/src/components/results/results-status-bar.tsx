// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useCallback } from 'react';
import { format_value } from '../../format/value';

interface ResultsStatusBarProps {
  row_count: number;
  execution_time: number;
  data?: Record<string, unknown>[];
}

export function ResultsStatusBar({ row_count, execution_time, data }: ResultsStatusBarProps) {
  const copy_as_csv = useCallback(() => {
    if (!data || data.length === 0) return;
    const columns = Object.keys(data[0]);
    const header = columns.join(',');
    const rows = data.map(row =>
      columns.map(col => {
        const val = format_value(row[col]);
        return val.includes(',') || val.includes('"') || val.includes('\n')
          ? `"${val.replace(/"/g, '""')}"`
          : val;
      }).join(',')
    );
    navigator.clipboard.writeText([header, ...rows].join('\n'));
  }, [data]);

  const copy_as_json = useCallback(() => {
    if (!data || data.length === 0) return;
    const serialized = data.map(row => {
      const obj: Record<string, string> = {};
      for (const [key, val] of Object.entries(row)) {
        obj[key] = format_value(val);
      }
      return obj;
    });
    navigator.clipboard.writeText(JSON.stringify(serialized, null, 2));
  }, [data]);

  return (
    <div className="rdb-status-bar">
      <div className="rdb-status-bar__info">
        <span>{row_count} row{row_count !== 1 ? 's' : ''}</span>
        <span>({execution_time}ms)</span>
      </div>
      <div className="rdb-status-bar__actions">
        <button className="rdb-status-bar__btn" onClick={copy_as_csv} title="Copy as CSV">
          [CSV]
        </button>
        <button className="rdb-status-bar__btn" onClick={copy_as_json} title="Copy as JSON">
          [JSON]
        </button>
      </div>
    </div>
  );
}
