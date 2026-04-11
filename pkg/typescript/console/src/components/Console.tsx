// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Client } from '@reifydb/client';
import type { editor } from 'monaco-editor';
import type { Executor, TransactionType } from '../types';
import { WsExecutor, type WsClient } from '../executor/ws-executor';
import { ConsoleProvider, useConsoleStore } from '../state/use-console-store';
import { loadHistory, saveHistory } from '../state/history';
import { SplitPane } from './layout/SplitPane';
import { TabBar } from './layout/TabBar';
import { QueryEditor } from './editor/QueryEditor';
import { EditorToolbar } from './editor/EditorToolbar';
import { ResultsPanel } from './results/ResultsPanel';
import { ShapeBrowser } from './shape/ShapeBrowser';
import { HistoryPanel } from './history/HistoryPanel';
import { ConnectionPanel } from './connection/ConnectionPanel';
import type { ConnectionMode, ConnectionStatus } from './connection/ConnectionPanel';

export type ConnectionConfig =
  | { mode: 'wasm' }
  | { mode: 'websocket'; url: string };

export type RdbTheme = 'light' | 'dark';

export interface ConsoleProps {
  executor: Executor;
  initial_code?: string;
  history_key?: string;
  connection?: ConnectionConfig;
  theme?: RdbTheme;
  monaco_theme?: string | editor.IStandaloneThemeData;
}

const TABS = [
  { id: 'results', label: 'Results' },
  { id: 'history', label: 'History' },
  { id: 'shape', label: 'Shape' },
];

const WS_URL_STORAGE_KEY = 'rdb-console-ws-url';

function ConsoleInner({ executor, history_key, connection, theme = 'light', monaco_theme }: { executor: Executor; history_key?: string; connection?: ConnectionConfig; theme?: RdbTheme; monaco_theme?: string | editor.IStandaloneThemeData }) {
  const { state, dispatch } = useConsoleStore();
  const connection_locked = connection != null;
  const locked_ws_url = connection?.mode === 'websocket' ? connection.url : null;

  const [connection_mode, set_connection_mode] = useState<ConnectionMode>(
    connection ? connection.mode : 'wasm',
  );
  const [ws_url, set_ws_url] = useState(() => {
    if (connection?.mode === 'websocket') return connection.url;
    try {
      return localStorage.getItem(WS_URL_STORAGE_KEY) || 'ws://localhost:8090';
    } catch {
      return 'ws://localhost:8090';
    }
  });
  const [connection_status, set_connection_status] = useState<ConnectionStatus>(
    connection?.mode === 'websocket' ? 'connecting' : 'connected',
  );
  const [connection_error, set_connection_error] = useState<string | null>(null);
  const [active_executor, set_active_executor] = useState<Executor>(executor);
  const [transaction_type, set_transaction_type] = useState<TransactionType>('query');
  const [show_connection_panel, set_show_connection_panel] = useState(false);
  const ws_client_ref = useRef<{ disconnect(): void } | null>(null);
  const reconnect_timer_ref = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Persist ws_url to localStorage (only when not locked)
  useEffect(() => {
    if (connection_locked) return;
    try {
      localStorage.setItem(WS_URL_STORAGE_KEY, ws_url);
    } catch {
      // ignore
    }
  }, [ws_url, connection_locked]);

  // Keep active_executor in sync if prop changes while in wasm mode
  useEffect(() => {
    if (connection_mode === 'wasm') {
      set_active_executor(executor);
    }
  }, [executor, connection_mode]);

  // Auto-connect for locked websocket mode
  useEffect(() => {
    if (!locked_ws_url) return;
    const url = locked_ws_url;

    let cancelled = false;
    let backoff = 1000;
    const maxBackoff = 30000;

    async function connect() {
      if (cancelled) return;
      set_connection_status('connecting');
      set_connection_error(null);

      try {
        if (ws_client_ref.current) {
          ws_client_ref.current.disconnect();
          ws_client_ref.current = null;
        }

        const client = await Client.connect_ws(url, { timeout_ms: 30_000 });
        if (cancelled) {
          client.disconnect();
          return;
        }
        ws_client_ref.current = client;
        const ws_executor = new WsExecutor(client as unknown as WsClient);
        ws_executor.transaction_type = transaction_type;
        set_active_executor(ws_executor);
        set_connection_status('connected');
        backoff = 1000; // reset backoff on success
      } catch (err) {
        if (cancelled) return;
        set_connection_status('error');
        set_connection_error(err instanceof Error ? err.message : String(err));
        // Auto-reconnect with backoff
        reconnect_timer_ref.current = setTimeout(() => {
          connect();
        }, backoff);
        backoff = Math.min(backoff * 2, maxBackoff);
      }
    }

    connect();

    return () => {
      cancelled = true;
      if (reconnect_timer_ref.current) {
        clearTimeout(reconnect_timer_ref.current);
        reconnect_timer_ref.current = null;
      }
      if (ws_client_ref.current) {
        ws_client_ref.current.disconnect();
        ws_client_ref.current = null;
      }
    };
  }, [locked_ws_url]);

  // Load history on mount
  useEffect(() => {
    const entries = loadHistory(history_key);
    if (entries.length > 0) {
      dispatch({ type: 'LOAD_HISTORY', entries });
    }
  }, [history_key, dispatch]);

  // Save history on change
  useEffect(() => {
    saveHistory(state.history, history_key);
  }, [state.history, history_key]);

  const handleConnect = useCallback(async () => {
    if (!ws_url.trim()) return;
    set_connection_status('connecting');
    set_connection_error(null);

    try {
      // Disconnect previous WS client if any
      if (ws_client_ref.current) {
        ws_client_ref.current.disconnect();
        ws_client_ref.current = null;
      }

      const client = await Client.connect_ws(ws_url, { timeout_ms: 30_000 });
      ws_client_ref.current = client;
      const ws_executor = new WsExecutor(client as unknown as WsClient);
      ws_executor.transaction_type = transaction_type;
      set_active_executor(ws_executor);
      set_connection_status('connected');
    } catch (err) {
      set_connection_status('error');
      set_connection_error(err instanceof Error ? err.message : String(err));
    }
  }, [ws_url, transaction_type]);

  const handle_disconnect = useCallback(() => {
    if (ws_client_ref.current) {
      ws_client_ref.current.disconnect();
      ws_client_ref.current = null;
    }
    set_active_executor(executor);
    set_connection_mode('wasm');
    set_connection_status('connected');
    set_connection_error(null);
  }, [executor]);

  const handleTransactionTypeChange = useCallback((type: TransactionType) => {
    set_transaction_type(type);
    if (active_executor instanceof WsExecutor) {
      active_executor.transaction_type = type;
    }
  }, [active_executor]);

  const handleModeChange = useCallback((mode: ConnectionMode) => {
    if (mode === 'wasm' && connection_mode === 'websocket') {
      // Switching back to wasm — disconnect if connected
      if (ws_client_ref.current) {
        ws_client_ref.current.disconnect();
        ws_client_ref.current = null;
      }
      set_active_executor(executor);
      set_connection_status('connected');
      set_connection_error(null);
    } else if (mode === 'websocket' && connection_mode === 'wasm') {
      // Switching to websocket mode — not connected yet
      set_connection_status('disconnected');
      set_connection_error(null);
    }
    set_connection_mode(mode);
  }, [connection_mode, executor]);

  const resolvedMonacoThemeName = useMemo(() => {
    if (!monaco_theme) return undefined;
    if (typeof monaco_theme === 'string') return monaco_theme;
    return 'rdb-custom';
  }, [monaco_theme]);

  const resolvedMonacoThemeData = useMemo(() => {
    if (!monaco_theme || typeof monaco_theme === 'string') return undefined;
    return monaco_theme;
  }, [monaco_theme]);

  const connectionLabel = connection_mode === 'wasm' ? 'wasm' : ws_url;

  const handleRun = useCallback(async () => {
    if (state.is_executing || !state.code.trim()) return;
    dispatch({ type: 'EXECUTE_START' });

    try {
      const result = await active_executor.execute(state.code);
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
          execution_time: 0,
        },
        query: state.code,
      });
    }

  }, [state.is_executing, state.code, active_executor, dispatch]);

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
          is_executing={state.is_executing}
          connectionLabel={connectionLabel}
          connection_status={connection_status}
          connection_locked={connection_locked}
          onToggleConnectionPanel={() => set_show_connection_panel((v) => !v)}
          connection_mode={connection_mode}
          transaction_type={transaction_type}
          onTransactionTypeChange={handleTransactionTypeChange}
        />
        {!connection_locked && show_connection_panel && (
          <ConnectionPanel
            mode={connection_mode}
            ws_url={ws_url}
            status={connection_status}
            error={connection_error}
            onModeChange={handleModeChange}
            onUrlChange={set_ws_url}
            onConnect={handleConnect}
            onDisconnect={handle_disconnect}
            onClose={() => set_show_connection_panel(false)}
          />
        )}
      </div>
      <div style={{ flex: 1, minHeight: 0 }}>
        <QueryEditor
          code={state.code}
          onChange={(code) => dispatch({ type: 'SET_CODE', code })}
          onRun={handleRun}
          theme={theme}
          monacoThemeName={resolvedMonacoThemeName}
          monacoThemeData={resolvedMonacoThemeData}
        />
      </div>
    </div>
  );

  const bottomPane = (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      <TabBar
        activeTab={state.activeTab}
        tabs={TABS}
        onTabChange={(tab) => dispatch({ type: 'SET_TAB', tab: tab as 'results' | 'history' | 'shape' })}
      />
      <div style={{ flex: 1, overflow: 'auto', minHeight: 0 }}>
        {state.activeTab === 'results' ? (
          <ResultsPanel result={state.result} />
        ) : state.activeTab === 'history' ? (
          <HistoryPanel entries={state.history} onSelect={handleSelectHistory} />
        ) : (
          <ShapeBrowser executor={active_executor} />
        )}
      </div>
    </div>
  );

  return (
    <div className={`rdb-console${theme === 'light' ? ' rdb-theme-light' : ''}`}>
      <div className="rdb-console__main">
        <SplitPane top={editorPane} bottom={bottomPane} initialSplit={45} />
      </div>
    </div>
  );
}

export function Console({ executor, initial_code, history_key, connection, theme, monaco_theme }: ConsoleProps) {
  return (
    <ConsoleProvider initial_code={initial_code}>
      <ConsoleInner executor={executor} history_key={history_key} connection={connection} theme={theme} monaco_theme={monaco_theme} />
    </ConsoleProvider>
  );
}
