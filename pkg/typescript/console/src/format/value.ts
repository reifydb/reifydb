// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { format_value, value_role, type ValueRole } from '@reifydb/core';

export { format_value };

export interface ValueStyle {
  color?: string;
  italic?: boolean;
}

const STYLE_BY_ROLE: Record<ValueRole, ValueStyle> = {
  none: { color: 'var(--rdb-color-muted)', italic: true },
  boolean: { color: 'var(--rdb-color-value-boolean)' },
  number: { color: 'var(--rdb-color-value-number)' },
  temporal: { color: 'var(--rdb-color-value-date)' },
  uuid: { color: 'var(--rdb-color-value-uuid)' },
  string: { color: 'var(--rdb-color-value-string)' },
  blob: { color: 'var(--rdb-color-secondary)' },
  unknown: {},
};

export function get_value_style(value: unknown): ValueStyle {
  return STYLE_BY_ROLE[value_role(value)];
}
