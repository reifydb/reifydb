// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import type {ShapeNode} from '@reifydb/core';
import {useRunner} from './use-runner';
import type {Runner} from './use-runner';

export type UseCommandResult<S extends readonly ShapeNode[]> = Runner<S>;

export function useCommand<const S extends readonly ShapeNode[]>(shapes: S): UseCommandResult<S> {
    return useRunner('command', shapes);
}
