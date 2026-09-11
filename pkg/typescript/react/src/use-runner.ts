// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {useCallback, useState} from 'react';
import type {FrameResults, ShapeNode} from '@reifydb/core';
import {useStore} from './provider';

export interface Runner<S extends readonly ShapeNode[]> {
    run: (rql: string, params?: any) => Promise<FrameResults<S>>;
    isPending: boolean;
    error: Error | undefined;
}

function toError(error: unknown): Error {
    return error instanceof Error ? error : new Error(String(error));
}

// Nothing runs until run() is called, so a statement that writes or changes the catalog never fires on mount.
export function useRunner<const S extends readonly ShapeNode[]>(kind: 'admin' | 'command', shapes: S): Runner<S> {
    const store = useStore();
    const [pending, setPending] = useState(0);
    const [error, setError] = useState<Error | undefined>(undefined);
    const run = useCallback(async (rql: string, params: any = null): Promise<FrameResults<S>> => {
        setPending(count => count + 1);
        try {
            const result = await store[kind](rql, params, shapes);
            setError(undefined);
            return result;
        } catch (cause) {
            setError(toError(cause));
            throw cause;
        } finally {
            setPending(count => count - 1);
        }
    }, [store, kind, shapes]);
    return {run, isPending: pending > 0, error};
}
