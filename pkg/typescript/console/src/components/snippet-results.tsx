// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { format_value, get_value_style } from '../format/value';

interface SnippetResultsProps {
  data: Record<string, unknown>[];
  columns: string[];
}

export function SnippetResults({ data, columns }: SnippetResultsProps) {
  return (
    <>
      <div className="rdb-snippet__table-wrap">
        <table className="rdb-snippet__table">
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
                      style={{ color: vs.color, fontStyle: vs.italic ? 'italic' : undefined }}
                    >{format_value(row[col])}</td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="rdb-snippet__rows">
        {data.map((row, i) => (
          <div key={i} className="rdb-snippet__record">
            <div className="rdb-snippet__record-label">row {String(i + 1).padStart(2, '0')}</div>
            <table className="rdb-snippet__record-table">
              <tbody>
                {columns.map((col) => {
                  const vs = get_value_style(row[col]);
                  return (
                    <tr key={col}>
                      <th scope="row" className="rdb-snippet__record-key">{col}</th>
                      <td
                        className="rdb-snippet__record-value"
                        style={{ color: vs.color, fontStyle: vs.italic ? 'italic' : undefined }}
                      >{format_value(row[col])}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ))}
      </div>
    </>
  );
}
