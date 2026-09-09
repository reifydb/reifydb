// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import type {ReactNode} from 'react';
import {Store} from '@reifydb/store';
import {StoreProvider} from '../src';
import {FakeClient} from './fake-client';

export function setup() {
    const client = new FakeClient();
    const store = new Store(client);
    return {client, store, wrapper: withStore(store)};
}

export function withStore(store: Store) {
    return ({children}: {children: ReactNode}) => <StoreProvider store={store}>{children}</StoreProvider>;
}
