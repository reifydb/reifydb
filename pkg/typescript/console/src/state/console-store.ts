// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { ExecutionResult, HistoryEntry } from '../types';

export interface ConsoleState {
  code: string;
  is_executing: boolean;
  result: ExecutionResult | null;
  history: HistoryEntry[];
  active_tab: 'results' | 'history' | 'catalog';
}

export type ConsoleAction =
  | { type: 'SET_CODE'; code: string }
  | { type: 'EXECUTE_START' }
  | { type: 'EXECUTE_SUCCESS'; result: ExecutionResult; query: string }
  | { type: 'EXECUTE_ERROR'; result: ExecutionResult; query: string }
  | { type: 'CLEAR_RESULTS' }
  | { type: 'SET_TAB'; tab: 'results' | 'history' | 'catalog' }
  | { type: 'LOAD_HISTORY'; entries: HistoryEntry[] }
  | { type: 'CLEAR_HISTORY' }
  | { type: 'LOAD_QUERY'; code: string };

let next_history_id = 1;

export function console_reducer(state: ConsoleState, action: ConsoleAction): ConsoleState {
  switch (action.type) {
    case 'SET_CODE':
      return { ...state, code: action.code };

    case 'EXECUTE_START':
      return { ...state, is_executing: true, result: null };

    case 'EXECUTE_SUCCESS': {
      const entry: HistoryEntry = {
        id: String(next_history_id++),
        query: action.query,
        timestamp: Date.now(),
        success: true,
        row_count: action.result.data?.length,
        execution_time: action.result.execution_time,
      };
      return {
        ...state,
        is_executing: false,
        result: action.result,
        history: [entry, ...state.history],
        active_tab: 'results',
      };
    }

    case 'EXECUTE_ERROR': {
      const entry: HistoryEntry = {
        id: String(next_history_id++),
        query: action.query,
        timestamp: Date.now(),
        success: false,
        execution_time: action.result.execution_time,
      };
      return {
        ...state,
        is_executing: false,
        result: action.result,
        history: [entry, ...state.history],
        active_tab: 'results',
      };
    }

    case 'CLEAR_RESULTS':
      return { ...state, result: null };

    case 'SET_TAB':
      return { ...state, active_tab: action.tab };

    case 'LOAD_HISTORY':
      return { ...state, history: action.entries };

    case 'CLEAR_HISTORY':
      return { ...state, history: [] };

    case 'LOAD_QUERY':
      return { ...state, code: action.code, active_tab: 'results' };

    default:
      return state;
  }
}

export const initial_console_state: ConsoleState = {
  code: '',
  is_executing: false,
  result: null,
  history: [],
  active_tab: 'results',
};
