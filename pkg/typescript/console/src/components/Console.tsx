// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

import { useCallback, useEffect } from 'react';
import type { Executor } from '../types';
import { ConsoleProvider, useConsoleStore } from '../state/use-console-store';
import { loadHistory, saveHistory } from '../state/history';
import { SplitPane } from './layout/SplitPane';
import { TabBar } from './layout/TabBar';
import { QueryEditor } from './editor/QueryEditor';
import { EditorToolbar } from './editor/EditorToolbar';
import { ResultsPanel } from './results/ResultsPanel';
import { SchemaBrowser } from './schema/SchemaBrowser';
import { HistoryPanel } from './history/HistoryPanel';

export interface ConsoleProps {
  executor: Executor;
  initialCode?: string;
  historyKey?: string;
}

const TABS = [
  { id: 'results', label: 'Results' },
  { id: 'history', label: 'History' },
  { id: 'schema', label: 'Schema' },
];

function ConsoleInner({ executor, historyKey }: { executor: Executor; historyKey?: string }) {
  const { state, dispatch } = useConsoleStore();

  // Load history on mount
  useEffect(() => {
    const entries = loadHistory(historyKey);
    if (entries.length > 0) {
      dispatch({ type: 'LOAD_HISTORY', entries });
    }
  }, [historyKey, dispatch]);

  // Save history on change
  useEffect(() => {
    saveHistory(state.history, historyKey);
  }, [state.history, historyKey]);

  const handleRun = useCallback(async () => {
    if (state.isExecuting || !state.code.trim()) return;
    dispatch({ type: 'EXECUTE_START' });

    try {
      const result = await executor.execute(state.code);
      if (result.success) {
        dispatch({ type: 'EXECUTE_SUCCESS', result, query: state.code });
      } else {
        dispatch({ type: 'EXECUTE_ERROR', result, query: state.code });
      }
    } catch (err) {
      dispatch({
        type: 'EXECUTE_ERROR',
        result: {
          success: false,
          error: err instanceof Error ? err.message : String(err),
          executionTime: 0,
        },
        query: state.code,
      });
    }

  }, [state.isExecuting, state.code, executor, dispatch]);

  const handleClear = useCallback(() => {
    dispatch({ type: 'CLEAR_RESULTS' });
  }, [dispatch]);

  const handleSelectHistory = useCallback((query: string) => {
    dispatch({ type: 'LOAD_QUERY', code: query });
  }, [dispatch]);

  const editorPane = (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      <EditorToolbar onRun={handleRun} onClear={handleClear} isExecuting={state.isExecuting} />
      <div style={{ flex: 1, minHeight: 0 }}>
        <QueryEditor
          code={state.code}
          onChange={(code) => dispatch({ type: 'SET_CODE', code })}
          onRun={handleRun}
        />
      </div>
    </div>
  );

  const bottomPane = (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      <TabBar
        activeTab={state.activeTab}
        tabs={TABS}
        onTabChange={(tab) => dispatch({ type: 'SET_TAB', tab: tab as 'results' | 'history' | 'schema' })}
      />
      <div style={{ flex: 1, overflow: 'auto', minHeight: 0 }}>
        {state.activeTab === 'results' ? (
          <ResultsPanel result={state.result} />
        ) : state.activeTab === 'history' ? (
          <HistoryPanel entries={state.history} onSelect={handleSelectHistory} />
        ) : (
          <SchemaBrowser executor={executor} />
        )}
      </div>
    </div>
  );

  return (
    <div className="rdb-console">
      <div className="rdb-console__main">
        <SplitPane top={editorPane} bottom={bottomPane} initialSplit={45} />
      </div>
    </div>
  );
}

export function Console({ executor, initialCode, historyKey }: ConsoleProps) {
  return (
    <ConsoleProvider initialCode={initialCode}>
      <ConsoleInner executor={executor} historyKey={historyKey} />
    </ConsoleProvider>
  );
}
