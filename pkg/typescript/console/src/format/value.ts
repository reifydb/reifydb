// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB


export interface ValueStyle {
  color?: string;
  italic?: boolean;
}

export function getValueStyle(value: unknown): ValueStyle {
  if (value === null || value === undefined) {
    return { color: 'var(--rdb-color-muted)', italic: true };
  }

  // Duck-type on `type` property - Value classes use `implements` (not extends),
  // so instanceof fails. Every concrete Value sets this.type in its constructor.
  const t = (value as { type?: unknown }).type;
  if (typeof t === 'string') {
    switch (t) {
      case 'None':
        return { color: 'var(--rdb-color-muted)', italic: true };
      case 'Boolean':
        return { color: '#818CF8' };
      case 'Int1': case 'Int2': case 'Int4': case 'Int8': case 'Int16':
      case 'Uint1': case 'Uint2': case 'Uint4': case 'Uint8': case 'Uint16':
      case 'Float4': case 'Float8': case 'Decimal':
        return { color: '#F472B6' };
      case 'Date': case 'DateTime': case 'Time': case 'Duration':
        return { color: '#06B6D4' };
      case 'Uuid4': case 'Uuid7': case 'IdentityId':
        return { color: '#14B8A6' };
      case 'Utf8':
        return { color: '#34D399' };
      case 'Blob':
        return { color: 'var(--rdb-color-secondary)' };
    }
  }

  // Fallback: color by JS typeof
  switch (typeof value) {
    case 'number':
    case 'bigint':
      return { color: '#F472B6' };
    case 'boolean':
      return { color: '#818CF8' };
    case 'string':
      return { color: '#34D399' };
    default:
      return {};
  }
}

export function formatValue(value: unknown): string {
  if (value === null || value === undefined) {
    return 'none';
  }
  // Duck-type Value objects by checking for .type (same approach as getValueStyle).
  // Handles BigInt-backed types (Int8, Uint8, etc.) that crash JSON.stringify.
  if (typeof value === 'object' && typeof (value as any).type === 'string') {
    return value.toString();
  }
  if (typeof value === 'bigint') {
    return value.toString();
  }
  if (typeof value === 'object') {
    return JSON.stringify(value);
  }
  return String(value);
}
