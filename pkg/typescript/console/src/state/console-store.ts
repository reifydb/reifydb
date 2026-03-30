// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { ExecutionResult, HistoryEntry } from '../types';

export interface ConsoleState {
  code: string;
  isExecuting: boolean;
  result: ExecutionResult | null;
  history: HistoryEntry[];
  activeTab: 'results' | 'history' | 'shape';
}

export type ConsoleAction =
  | { type: 'SET_CODE'; code: string }
  | { type: 'EXECUTE_START' }
  | { type: 'EXECUTE_SUCCESS'; result: ExecutionResult; query: string }
  | { type: 'EXECUTE_ERROR'; result: ExecutionResult; query: string }
  | { type: 'CLEAR_RESULTS' }
  | { type: 'SET_TAB'; tab: 'results' | 'history' | 'shape' }
  | { type: 'LOAD_HISTORY'; entries: HistoryEntry[] }
  | { type: 'CLEAR_HISTORY' }
  | { type: 'LOAD_QUERY'; code: string };

let nextHistoryId = 1;

export function consoleReducer(state: ConsoleState, action: ConsoleAction): ConsoleState {
  switch (action.type) {
    case 'SET_CODE':
      return { ...state, code: action.code };

    case 'EXECUTE_START':
      return { ...state, isExecuting: true, result: null };

    case 'EXECUTE_SUCCESS': {
      const entry: HistoryEntry = {
        id: String(nextHistoryId++),
        query: action.query,
        timestamp: Date.now(),
        success: true,
        rowCount: action.result.data?.length,
        executionTime: action.result.executionTime,
      };
      return {
        ...state,
        isExecuting: false,
        result: action.result,
        history: [entry, ...state.history],
        activeTab: 'results',
      };
    }

    case 'EXECUTE_ERROR': {
      const entry: HistoryEntry = {
        id: String(nextHistoryId++),
        query: action.query,
        timestamp: Date.now(),
        success: false,
        executionTime: action.result.executionTime,
      };
      return {
        ...state,
        isExecuting: false,
        result: action.result,
        history: [entry, ...state.history],
        activeTab: 'results',
      };
    }

    case 'CLEAR_RESULTS':
      return { ...state, result: null };

    case 'SET_TAB':
      return { ...state, activeTab: action.tab };

    case 'LOAD_HISTORY':
      return { ...state, history: action.entries };

    case 'CLEAR_HISTORY':
      return { ...state, history: [] };

    case 'LOAD_QUERY':
      return { ...state, code: action.code, activeTab: 'results' };

    default:
      return state;
  }
}

export const initialConsoleState: ConsoleState = {
  code: '',
  isExecuting: false,
  result: null,
  history: [],
  activeTab: 'results',
};
