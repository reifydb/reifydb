// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { format_value, get_value_style } from '../../format/value';

interface ResultsTableProps {
  data: Record<string, unknown>[];
}

export function ResultsTable({ data }: ResultsTableProps) {
  if (data.length === 0) return null;

  const columns = Object.keys(data[0]);

  return (
    <div className="rdb-results">
      <table className="rdb-results__table">
        <thead>
          <tr>
            {columns.map((col) => (
              <th key={col}>{col}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((row, i) => (
            <tr key={i}>
              {columns.map((col) => {
                const vs = get_value_style(row[col]);
                return (
                  <td
                    key={col}
                    style={{
                      color: vs.color,
                      fontStyle: vs.italic ? 'italic' : undefined,
                    }}
                  >
                    {format_value(row[col])}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
