// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

import { formatValue, getValueStyle } from '../../format/value';

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
                const vs = getValueStyle(row[col]);
                return (
                  <td
                    key={col}
                    style={{
                      color: vs.color,
                      fontStyle: vs.italic ? 'italic' : undefined,
                    }}
                  >
                    {formatValue(row[col])}
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
