// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { createContext, useContext, useReducer, type Dispatch, type ReactNode } from 'react';
import { consoleReducer, initialConsoleState, type ConsoleState, type ConsoleAction } from './console-store';

interface ConsoleContextValue {
  state: ConsoleState;
  dispatch: Dispatch<ConsoleAction>;
}

const ConsoleContext = createContext<ConsoleContextValue | null>(null);

interface ConsoleProviderProps {
  children: ReactNode;
  initial_code?: string;
}

export function ConsoleProvider({ children, initial_code }: ConsoleProviderProps) {
  const [state, dispatch] = useReducer(consoleReducer, {
    ...initialConsoleState,
    code: initial_code ?? '',
  });

  return (
    <ConsoleContext.Provider value={{ state, dispatch }}>
      {children}
    </ConsoleContext.Provider>
  );
}

export function useConsoleStore(): ConsoleContextValue {
  const ctx = useContext(ConsoleContext);
  if (!ctx) {
    throw new Error('useConsoleStore must be used within a ConsoleProvider');
  }
  return ctx;
}
