// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

import { formatValue, getValueStyle } from '../format/value';

interface SnippetResultsProps {
  data: Record<string, unknown>[];
  columns: string[];
  maxKeyLength: number;
}

export function SnippetResults({ data, columns, maxKeyLength }: SnippetResultsProps) {
  return (
    <div className="rdb-snippet__rows">
      {data.map((row, i) => (
        <div key={i} className="rdb-snippet__row">
          <div className="rdb-snippet__row-label">-- row {i + 1} --</div>
          {columns.map((col) => {
            const vs = getValueStyle(row[col]);
            return (
              <div key={col} className="rdb-snippet__field">
                <span
                  className="rdb-snippet__field-key"
                  style={{ minWidth: `${maxKeyLength}ch` }}
                >{`  ${col.padEnd(maxKeyLength)}`}</span>
                <span className="rdb-snippet__field-eq">= </span>
                <span
                  className={`rdb-snippet__field-value${vs.italic ? ' rdb-snippet__field-value--italic' : ''}`}
                  style={vs.color ? { color: vs.color } : undefined}
                >{formatValue(row[col])}</span>
              </div>
            );
          })}
        </div>
      ))}
    </div>
  );
}
