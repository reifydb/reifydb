// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import type {ShapeNode} from '@reifydb/core';
import type {WriteSpec} from '@reifydb/store';
import {useRunner} from './use-runner';
import type {Runner} from './use-runner';

export type UseAdminResult<S extends readonly ShapeNode[], P extends object | null> = Runner<S, P>;

// Admin carries the rights that DDL needs, and reads run through it too; the result is returned, never cached.
export function useAdmin<S extends readonly ShapeNode[], P extends object | null>(spec: WriteSpec<S, P>): UseAdminResult<S, P> {
    return useRunner('admin', spec);
}
