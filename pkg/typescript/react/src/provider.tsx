// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {createContext, useContext} from 'react';
import type {ReactNode} from 'react';
import type {Store} from '@reifydb/store';

const StoreContext = createContext<Store | undefined>(undefined);

export interface StoreProviderProps {
    store: Store;
    children?: ReactNode;
}

export function StoreProvider({store, children}: StoreProviderProps) {
    return <StoreContext.Provider value={store}>{children}</StoreContext.Provider>;
}

export function useStore(): Store {
    const store = useContext(StoreContext);
    if (store === undefined) {
        throw new Error('useStore must be used inside a StoreProvider');
    }
    return store;
}
