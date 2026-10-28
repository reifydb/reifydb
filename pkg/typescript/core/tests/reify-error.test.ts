// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {ReifyError} from '../src';

const diagnostic = {code: 'QUERY_TIMEOUT', message: 'Query execution timed out', notes: []};

describe('ReifyError index', () => {
    it('carries the index of the subscribe item that failed', () => {
        // Without it a client cannot tell which item of its subscribe call failed.
        const error = new ReifyError({id: 's', type: 'Err', payload: {diagnostic, index: 1}} as any);

        expect(error.index).toBe(1);
    });

    it('names no item when the frame carries no index', () => {
        // A whole-call failure must name no item, or the client blames an innocent subscription.
        const error = new ReifyError({id: 's', type: 'Err', payload: {diagnostic}} as any);

        expect(error.index).toBeUndefined();
    });
});
