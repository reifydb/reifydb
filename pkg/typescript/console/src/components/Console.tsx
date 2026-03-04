// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

import { useCallback, useEffect, useRef, useState } from 'react';
import { Client } from '@reifydb/client';
import type { Executor } from '../types';
import { WsExecutor, type WsClient } from '../executor/ws-executor';
import { ConsoleProvider, useConsoleStore } from '../state/use-console-store';
import { loadHistory, saveHistory } from '../state/history';
import { SplitPane } from './layout/SplitPane';
import { TabBar } from './layout/TabBar';
import { QueryEditor } from './editor/QueryEditor';
import { EditorToolbar } from './editor/EditorToolbar';
import { ResultsPanel } from './results/ResultsPanel';
import { SchemaBrowser } from './schema/SchemaBrowser';
import { HistoryPanel } from './history/HistoryPanel';
import { ConnectionPanel } from './connection/ConnectionPanel';
import type { ConnectionMode, ConnectionStatus } from './connection/ConnectionPanel';

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

const WS_URL_STORAGE_KEY = 'rdb-console-ws-url';

function ConsoleInner({ executor, historyKey }: { executor: Executor; historyKey?: string }) {
  const { state, dispatch } = useConsoleStore();

  const [connectionMode, setConnectionMode] = useState<ConnectionMode>('wasm');
  const [wsUrl, setWsUrl] = useState(() => {
    try {
      return localStorage.getItem(WS_URL_STORAGE_KEY) || 'ws://localhost:8090';
    } catch {
      return 'ws://localhost:8090';
    }
  });
  const [connectionStatus, setConnectionStatus] = useState<ConnectionStatus>('connected');
  const [connectionError, setConnectionError] = useState<string | null>(null);
  const [activeExecutor, setActiveExecutor] = useState<Executor>(executor);
  const [showConnectionPanel, setShowConnectionPanel] = useState(false);
  const wsClientRef = useRef<{ disconnect(): void } | null>(null);

  // Persist wsUrl to localStorage
  useEffect(() => {
    try {
      localStorage.setItem(WS_URL_STORAGE_KEY, wsUrl);
    } catch {
      // ignore
    }
  }, [wsUrl]);

  // Keep activeExecutor in sync if prop changes while in wasm mode
  useEffect(() => {
    if (connectionMode === 'wasm') {
      setActiveExecutor(executor);
    }
  }, [executor, connectionMode]);

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

  const handleConnect = useCallback(async () => {
    if (!wsUrl.trim()) return;
    setConnectionStatus('connecting');
    setConnectionError(null);

    try {
      // Disconnect previous WS client if any
      if (wsClientRef.current) {
        wsClientRef.current.disconnect();
        wsClientRef.current = null;
      }

      const client = await Client.connect_ws(wsUrl, { timeoutMs: 30_000 });
      wsClientRef.current = client;
      const wsExecutor = new WsExecutor(client as unknown as WsClient);
      setActiveExecutor(wsExecutor);
      setConnectionStatus('connected');
    } catch (err) {
      setConnectionStatus('error');
      setConnectionError(err instanceof Error ? err.message : String(err));
    }
  }, [wsUrl]);

  const handleDisconnect = useCallback(() => {
    if (wsClientRef.current) {
      wsClientRef.current.disconnect();
      wsClientRef.current = null;
    }
    setActiveExecutor(executor);
    setConnectionMode('wasm');
    setConnectionStatus('connected');
    setConnectionError(null);
  }, [executor]);

  const handleModeChange = useCallback((mode: ConnectionMode) => {
    if (mode === 'wasm' && connectionMode === 'websocket') {
      // Switching back to wasm — disconnect if connected
      if (wsClientRef.current) {
        wsClientRef.current.disconnect();
        wsClientRef.current = null;
      }
      setActiveExecutor(executor);
      setConnectionStatus('connected');
      setConnectionError(null);
    } else if (mode === 'websocket' && connectionMode === 'wasm') {
      // Switching to websocket mode — not connected yet
      setConnectionStatus('disconnected');
      setConnectionError(null);
    }
    setConnectionMode(mode);
  }, [connectionMode, executor]);

  const connectionLabel = connectionMode === 'wasm' ? 'wasm' : wsUrl;

  const handleRun = useCallback(async () => {
    if (state.isExecuting || !state.code.trim()) return;
    dispatch({ type: 'EXECUTE_START' });

    try {
      const result = await activeExecutor.execute(state.code);
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

  }, [state.isExecuting, state.code, activeExecutor, dispatch]);

  const handleClear = useCallback(() => {
    dispatch({ type: 'CLEAR_RESULTS' });
  }, [dispatch]);

  const handleSelectHistory = useCallback((query: string) => {
    dispatch({ type: 'LOAD_QUERY', code: query });
  }, [dispatch]);

  const editorPane = (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      <div style={{ position: 'relative' }}>
        <EditorToolbar
          onRun={handleRun}
          onClear={handleClear}
          isExecuting={state.isExecuting}
          connectionLabel={connectionLabel}
          connectionStatus={connectionStatus}
          onToggleConnectionPanel={() => setShowConnectionPanel((v) => !v)}
        />
        {showConnectionPanel && (
          <ConnectionPanel
            mode={connectionMode}
            wsUrl={wsUrl}
            status={connectionStatus}
            error={connectionError}
            onModeChange={handleModeChange}
            onUrlChange={setWsUrl}
            onConnect={handleConnect}
            onDisconnect={handleDisconnect}
            onClose={() => setShowConnectionPanel(false)}
          />
        )}
      </div>
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
          <SchemaBrowser executor={activeExecutor} />
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
