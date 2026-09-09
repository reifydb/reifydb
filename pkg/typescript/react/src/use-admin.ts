// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import type {ShapeNode} from '@reifydb/core';
import {useRunner} from './use-runner';
import type {Runner} from './use-runner';

export type UseAdminResult<S extends readonly ShapeNode[]> = Runner<S>;

// Admin carries the rights that DDL needs, and reads run through it too; the result is returned, never cached.
export function useAdmin<const S extends readonly ShapeNode[]>(shapes: S): UseAdminResult<S> {
    return useRunner('admin', shapes);
}
