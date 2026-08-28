// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { NONE_PRESENTATION, formatValue, valueRole, type ValueRole } from '@reifydb/core';

export { formatValue };

export interface ValueStyle {
  color?: string;
  italic?: boolean;
}

const STYLE_BY_ROLE: Record<ValueRole, ValueStyle> = {
  none: {
    color: NONE_PRESENTATION.muted ? 'var(--rdb-color-none)' : undefined,
    italic: NONE_PRESENTATION.italic,
  },
  boolean: { color: 'var(--rdb-color-value-boolean)' },
  number: { color: 'var(--rdb-color-value-number)' },
  temporal: { color: 'var(--rdb-color-value-date)' },
  uuid: { color: 'var(--rdb-color-value-uuid)' },
  string: { color: 'var(--rdb-color-value-string)' },
  blob: { color: 'var(--rdb-color-secondary)' },
  unknown: {},
};

export function getValueStyle(value: unknown): ValueStyle {
  return STYLE_BY_ROLE[valueRole(value)];
}
