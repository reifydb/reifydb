// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
import { expect } from 'vitest';
import type { ResponseMeta } from '../../src/types';

export function assertMeta(meta: ResponseMeta, fingerprint: string): void {
    expect(meta.fingerprint).toBe(fingerprint);
    expect(meta.duration).toMatch(/^(\d+(ns|us|ms|mo|[smhdy]))+$/);
}
