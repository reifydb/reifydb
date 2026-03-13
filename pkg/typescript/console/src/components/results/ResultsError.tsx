// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { Diagnostic } from '../../types';

interface ResultsErrorProps {
  message: string;
  diagnostic?: Diagnostic;
}

function getLine(source: string, line: number): string {
  return source.split('\n')[line - 1] ?? '';
}

function renderFlat(d: Diagnostic): string {
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

    const lineContent = getLine(statement, line);

    lines.push('CODE');
    lines.push(`  ${line} \u2502 ${lineContent}`);
    const fragmentStart = lineContent.indexOf(fragment) !== -1
      ? lineContent.indexOf(fragment)
      : column;
    lines.push(`    \u2502 ${' '.repeat(fragmentStart)}${'~'.repeat(fragment.length)}`);
    lines.push('    \u2502');

    const labelText = d.label ?? '';
    if (labelText) {
      const fragmentCenter = fragmentStart + Math.floor(fragment.length / 2);
      const labelHalf = Math.floor(labelText.length / 2);
      const labelOffset = labelHalf > fragmentCenter ? 0 : fragmentCenter - labelHalf;
      lines.push(`    \u2502 ${' '.repeat(labelOffset)}${labelText}`);
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

function renderNested(d: Diagnostic, depth: number): string {
  const lines: string[] = [];
  const indent = depth === 0 ? '' : '  ';
  const prefix = depth === 0 ? '' : '\u21b3 ';

  lines.push(`${indent}${prefix}Error ${d.code}: ${d.message}`);

  if (d.fragment?.line != null && d.fragment.column != null) {
    const { text: fragment, line, column } = d.fragment;
    const statement = d.statement ?? '';

    const atText = statement ? `"${fragment}"` : 'unknown';
    lines.push(`${indent}  at ${atText} (line ${line}, column ${column})`);
    lines.push('');

    const lineContent = getLine(statement, line);
    lines.push(`${indent}  ${line} \u2502 ${lineContent}`);
    const fragmentStart = lineContent.indexOf(fragment) !== -1
      ? lineContent.indexOf(fragment)
      : column;
    lines.push(`${indent}    \u2502 ${' '.repeat(fragmentStart)}${'~'.repeat(fragment.length)}`);

    const labelText = d.label ?? '';
    if (labelText) {
      const fragmentCenter = fragmentStart + Math.floor(fragment.length / 2);
      const labelHalf = Math.floor(labelText.length / 2);
      const labelOffset = labelHalf > fragmentCenter ? 0 : fragmentCenter - labelHalf;
      lines.push(`${indent}    \u2502 ${' '.repeat(labelOffset)}${labelText}`);
    }
    lines.push('');
  }

  if (d.cause) {
    lines.push(renderNested(d.cause, depth + 1));
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

function renderDiagnostic(d: Diagnostic): string {
  if (d.cause) {
    return renderNested(d, 0);
  }
  return renderFlat(d);
}

export function ResultsError({ message, diagnostic }: ResultsErrorProps) {
  const text = diagnostic ? renderDiagnostic(diagnostic) : `Error\n  ${message}`;

  return (
    <div className="rdb-results__error">
      <pre>{text}</pre>
    </div>
  );
}
