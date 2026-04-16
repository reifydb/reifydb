// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { Diagnostic } from '../../types';

interface ResultsErrorProps {
  message: string;
  diagnostic?: Diagnostic;
}

function get_line(source: string, line: number): string {
  return source.split('\n')[line - 1] ?? '';
}

function render_flat(d: Diagnostic): string {
  const lines: string[] = [];

  lines.push(`Error ${d.code}`);
  lines.push(`  ${d.message}`);
  lines.push('');

  if (d.fragment?.line != null && d.fragment.column != null) {
    const { text: fragment, line, column } = d.fragment;
    const statement = d.statement ?? '';

    lines.push('LOCATION');
    lines.push(`  line ${line}, column ${column}`);
    lines.push('');

    const line_content = get_line(statement, line);

    lines.push('CODE');
    lines.push(`  ${line} \u2502 ${line_content}`);
    const fragment_start = line_content.indexOf(fragment) !== -1
      ? line_content.indexOf(fragment)
      : column;
    lines.push(`    \u2502 ${' '.repeat(fragment_start)}${'~'.repeat(fragment.length)}`);
    lines.push('    \u2502');

    const label_text = d.label ?? '';
    if (label_text) {
      const fragment_center = fragment_start + Math.floor(fragment.length / 2);
      const label_half = Math.floor(label_text.length / 2);
      const label_offset = label_half > fragment_center ? 0 : fragment_center - label_half;
      lines.push(`    \u2502 ${' '.repeat(label_offset)}${label_text}`);
    }
    lines.push('');
  }

  if (d.help) {
    lines.push('HELP');
    lines.push(`  ${d.help}`);
    lines.push('');
  }

  if (d.notes.length > 0) {
    lines.push('NOTES');
    for (const note of d.notes) {
      lines.push(`  \u2022 ${note}`);
    }
  }

  return lines.join('\n');
}

function render_nested(d: Diagnostic, depth: number): string {
  const lines: string[] = [];
  const indent = depth === 0 ? '' : '  ';
  const prefix = depth === 0 ? '' : '\u21b3 ';

  lines.push(`${indent}${prefix}Error ${d.code}: ${d.message}`);

  if (d.fragment?.line != null && d.fragment.column != null) {
    const { text: fragment, line, column } = d.fragment;
    const statement = d.statement ?? '';

    const at_text = statement ? `"${fragment}"` : 'unknown';
    lines.push(`${indent}  at ${at_text} (line ${line}, column ${column})`);
    lines.push('');

    const line_content = get_line(statement, line);
    lines.push(`${indent}  ${line} \u2502 ${line_content}`);
    const fragment_start = line_content.indexOf(fragment) !== -1
      ? line_content.indexOf(fragment)
      : column;
    lines.push(`${indent}    \u2502 ${' '.repeat(fragment_start)}${'~'.repeat(fragment.length)}`);

    const label_text = d.label ?? '';
    if (label_text) {
      const fragment_center = fragment_start + Math.floor(fragment.length / 2);
      const label_half = Math.floor(label_text.length / 2);
      const label_offset = label_half > fragment_center ? 0 : fragment_center - label_half;
      lines.push(`${indent}    \u2502 ${' '.repeat(label_offset)}${label_text}`);
    }
    lines.push('');
  }

  if (d.cause) {
    lines.push(render_nested(d.cause, depth + 1));
  }

  if (d.help) {
    lines.push(`${indent}  help: ${d.help}`);
  }

  if (d.notes.length > 0) {
    for (const note of d.notes) {
      lines.push(`${indent}  note: ${note}`);
    }
  }

  if (depth > 0) {
    lines.push('');
  }

  return lines.join('\n');
}

function render_diagnostic(d: Diagnostic): string {
  if (d.cause) {
    return render_nested(d, 0);
  }
  return render_flat(d);
}

export function ResultsError({ message, diagnostic }: ResultsErrorProps) {
  const text = diagnostic ? render_diagnostic(diagnostic) : `Error\n  ${message}`;

  return (
    <div className="rdb-results__error">
      <pre>{text}</pre>
    </div>
  );
}
