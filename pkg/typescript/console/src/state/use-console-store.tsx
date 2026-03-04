// SPDX-License-Identifier: AGPL-3.0-or-later
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
  initialCode?: string;
}

export function ConsoleProvider({ children, initialCode }: ConsoleProviderProps) {
  const [state, dispatch] = useReducer(consoleReducer, {
    ...initialConsoleState,
    code: initialCode ?? '',
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
