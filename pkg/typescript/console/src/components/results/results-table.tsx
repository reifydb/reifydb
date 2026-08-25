// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { infer_columns } from '@reifydb/core';
import { format_value, get_value_style } from '../../format/value';

interface ResultsTableProps {
  data: Record<string, unknown>[];
}

export function ResultsTable({ data }: ResultsTableProps) {
  if (data.length === 0) return null;

  const columns = infer_columns(data);

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
                const vs = get_value_style(row[col.name]);
                return (
                  <td
                    key={col.name}
                    style={{
                      color: vs.color,
                      fontStyle: vs.italic ? 'italic' : undefined,
                      textAlign: col.align,
                    }}
                  >
                    {format_value(row[col.name])}
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
