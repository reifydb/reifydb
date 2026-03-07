// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useCallback } from 'react';
import { formatValue } from '../../format/value';

interface ResultsStatusBarProps {
  rowCount: number;
  executionTime: number;
  data?: Record<string, unknown>[];
}

export function ResultsStatusBar({ rowCount, executionTime, data }: ResultsStatusBarProps) {
  const copyAsCsv = useCallback(() => {
    if (!data || data.length === 0) return;
    const columns = Object.keys(data[0]);
    const header = columns.join(',');
    const rows = data.map(row =>
      columns.map(col => {
        const val = formatValue(row[col]);
        return val.includes(',') || val.includes('"') || val.includes('\n')
          ? `"${val.replace(/"/g, '""')}"`
          : val;
      }).join(',')
    );
    navigator.clipboard.writeText([header, ...rows].join('\n'));
  }, [data]);

  const copyAsJson = useCallback(() => {
    if (!data || data.length === 0) return;
    const serialized = data.map(row => {
      const obj: Record<string, string> = {};
      for (const [key, val] of Object.entries(row)) {
        obj[key] = formatValue(val);
      }
      return obj;
    });
    navigator.clipboard.writeText(JSON.stringify(serialized, null, 2));
  }, [data]);

  return (
    <div className="rdb-status-bar">
      <div className="rdb-status-bar__info">
        <span>{rowCount} row{rowCount !== 1 ? 's' : ''}</span>
        <span>({executionTime}ms)</span>
      </div>
      <div className="rdb-status-bar__actions">
        <button className="rdb-status-bar__btn" onClick={copyAsCsv} title="Copy as CSV">
          [CSV]
        </button>
        <button className="rdb-status-bar__btn" onClick={copyAsJson} title="Copy as JSON">
          [JSON]
        </button>
      </div>
    </div>
  );
}
