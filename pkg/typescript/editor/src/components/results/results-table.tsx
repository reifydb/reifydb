// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { inferColumns } from '@reifydb/core';
import { formatValue, getValueStyle } from '../../format/value';

interface ResultsTableProps {
  data: Record<string, unknown>[];
}

export function ResultsTable({ data }: ResultsTableProps) {
  if (data.length === 0) return null;

  const columns = inferColumns(data);

  return (
    <div className="rdb-results">
      <table className="rdb-results__table">
        <thead>
          <tr>
            {columns.map((col) => (
              <th key={col.name} style={{ textAlign: col.align }}>
                {col.name}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((row, i) => (
            <tr key={i}>
              {columns.map((col) => {
                const vs = getValueStyle(row[col.name]);
                return (
                  <td
                    key={col.name}
                    style={{
                      color: vs.color,
                      fontStyle: vs.italic ? 'italic' : undefined,
                      textAlign: col.align,
                    }}
                  >
                    {formatValue(row[col.name])}
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
