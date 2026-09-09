// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expectTypeOf, it} from 'vitest';
import type {WsClient} from '@reifydb/client';
import type {StoreClient} from '../src';

describe('StoreClient', () => {
    it('is satisfied structurally by WsClient', () => {
        expectTypeOf<WsClient>().toMatchTypeOf<StoreClient>();
    });
});
