// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import type {ShapeNode} from '@reifydb/core';
import type {WriteSpec} from '@reifydb/store';
import {useRunner} from './use-runner';
import type {Runner} from './use-runner';

export type UseCommandResult<S extends readonly ShapeNode[], P extends object | null> = Runner<S, P>;

export function useCommand<S extends readonly ShapeNode[], P extends object | null>(spec: WriteSpec<S, P>): UseCommandResult<S, P> {
    return useRunner('command', spec);
}
