// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

import { Value } from '@reifydb/core';

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
  const str = value instanceof Value ? value.toString()
    : typeof value === 'object' ? JSON.stringify(value)
    : String(value);

  // Safety net: convert raw WASM DateTime repr to readable format
  const dtMatch = str.match(/DateTime\(.*?seconds:\s*(\d+)/);
  if (dtMatch) {
    const d = new Date(Number(dtMatch[1]) * 1000);
    return d.toISOString().slice(0, 19).replace('T', ' ') + ' UTC';
  }
  // Safety net: convert raw WASM Date repr to readable format
  const dateMatch = str.match(/Date\(.*?days_since_epoch:\s*(\d+)/);
  if (dateMatch) {
    return new Date(Number(dateMatch[1]) * 86400000).toISOString().slice(0, 10);
  }
  // Safety net: convert raw WASM Time repr to readable format
  const timeMatch = str.match(/Time\(.*?nanos_since_midnight:\s*(\d+)/);
  if (timeMatch) {
    const totalNanos = Number(timeMatch[1]);
    const totalSeconds = Math.floor(totalNanos / 1e9);
    const h = Math.floor(totalSeconds / 3600);
    const m = Math.floor((totalSeconds % 3600) / 60);
    const s = totalSeconds % 60;
    return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
  }
  // Safety net: convert raw WASM Duration repr to readable format
  const durMatch = str.match(/Duration\(.*?months:\s*(\d+).*?days:\s*(\d+).*?nanos:\s*(\d+)/);
  if (durMatch) {
    const months = Number(durMatch[1]);
    const days = Number(durMatch[2]);
    const nanos = Number(durMatch[3]);
    const totalSeconds = Math.floor(nanos / 1e9);
    const h = Math.floor(totalSeconds / 3600);
    const m = Math.floor((totalSeconds % 3600) / 60);
    const s = totalSeconds % 60;
    const parts: string[] = [];
    if (months > 0) parts.push(`${months} month${months !== 1 ? 's' : ''}`);
    if (days > 0) parts.push(`${days} day${days !== 1 ? 's' : ''}`);
    const time = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
    if (h > 0 || m > 0 || s > 0) parts.push(time);
    return parts.length > 0 ? parts.join(' ') : '0 days';
  }
  return str;
}
