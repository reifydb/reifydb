// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { format_value, get_value_style } from '../format/value';

interface SnippetResultsProps {
  data: Record<string, unknown>[];
  columns: string[];
  max_key_length: number;
}

export function SnippetResults({ data, columns, max_key_length }: SnippetResultsProps) {
  return (
    <div className="rdb-snippet__rows">
      {data.map((row, i) => (
        <div key={i} className="rdb-snippet__row">
          <div className="rdb-snippet__row-label">-- row {i + 1} --</div>
          {columns.map((col) => {
            const vs = get_value_style(row[col]);
            return (
              <div key={col} className="rdb-snippet__field">
                <span
                  className="rdb-snippet__field-key"
                  style={{ minWidth: `${max_key_length}ch` }}
                >{`  ${col.padEnd(max_key_length)}`}</span>
                <span className="rdb-snippet__field-eq">= </span>
                <span
                  className={`rdb-snippet__field-value${vs.italic ? ' rdb-snippet__field-value--italic' : ''}`}
                  style={vs.color ? { color: vs.color } : undefined}
                >{format_value(row[col])}</span>
              </div>
            );
          })}
        </div>
      ))}
    </div>
  );
}
