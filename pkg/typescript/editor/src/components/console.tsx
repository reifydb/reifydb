// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Client } from '@reifydb/client';
import type { editor } from 'monaco-editor';
import type { Executor, RdbTheme, TransactionType } from '../types';
import { WsExecutor, type WsClient } from '../executor/ws-executor';
import { ConsoleProvider, useConsoleStore } from '../state/use-console-store';
import { loadHistory, saveHistory } from '../state/history';
import { SplitPane } from './layout/split-pane';
import { TabBar } from './layout/tab-bar';
import { QueryEditor } from './editor/query-editor';
import { EditorToolbar } from './editor/editor-toolbar';
import { ResultsPanel } from './results/results-panel';
import { CatalogBrowser } from './catalog/catalog-browser';
import { HistoryPanel } from './history/history-panel';
import { ConnectionPanel } from './connection/connection-panel';
import type { ConnectionMode, ConnectionStatus } from './connection/connection-panel';

export type ConnectionConfig =
  | { mode: 'wasm' }
  | { mode: 'websocket'; url: string; connect?: (url: string) => Promise<WsClient> };

export type { RdbTheme };

const THEME_CLASSES: Record<RdbTheme, string> = {
  dark: '',
  light: ' rdb-theme-light',
  reifydb: ' rdb-theme-light rdb-theme-reifydb',
};

export function themeClass(theme: RdbTheme = 'light'): string {
  return THEME_CLASSES[theme];
}

export type CatalogPlacement = 'tab' | 'sidebar';

export interface ConsoleProps {
  executor: Executor;
  initialCode?: string;
  code?: string;
  autoRun?: boolean;
  historyKey?: string;
  connection?: ConnectionConfig;
  theme?: RdbTheme;
  monacoTheme?: string | editor.IStandaloneThemeData;
  transactionTypes?: readonly TransactionType[];
  catalogPlacement?: CatalogPlacement;
  catalogExpanded?: readonly string[];
  catalogNamespaces?: readonly string[];
}

const DEFAULT_TRANSACTION_TYPES: readonly TransactionType[] = ['query', 'command', 'admin'];

const TABS = [
  { id: 'results', label: 'Results' },
  { id: 'history', label: 'History' },
  { id: 'catalog', label: 'Catalog' },
];

const TABS_WITH_SIDEBAR = TABS.filter((tab) => tab.id !== 'catalog');

const WS_URL_STORAGE_KEY = 'rdb-console-ws-url';

function ConsoleInner({ executor, code, autoRun, historyKey, connection, theme = 'light', monacoTheme, transactionTypes = DEFAULT_TRANSACTION_TYPES, catalogPlacement = 'tab', catalogExpanded, catalogNamespaces }: { executor: Executor; code?: string; autoRun?: boolean; historyKey?: string; connection?: ConnectionConfig; theme?: RdbTheme; monacoTheme?: string | editor.IStandaloneThemeData; transactionTypes?: readonly TransactionType[]; catalogPlacement?: CatalogPlacement; catalogExpanded?: readonly string[]; catalogNamespaces?: readonly string[] }) {
  const { state, dispatch } = useConsoleStore();
  const connectionLocked = connection != null;
  const lockedWsUrl = connection?.mode === 'websocket' ? (connection.url ?? null) : null;
  const customConnect = connection?.mode === 'websocket' ? (connection.connect ?? null) : null;

  const [connectionMode, setConnectionMode] = useState<ConnectionMode>(
    connection ? connection.mode : 'wasm',
  );
  const [wsUrl, setWsUrl] = useState(() => {
    if (connection?.mode === 'websocket') return connection.url;
    try {
      return localStorage.getItem(WS_URL_STORAGE_KEY) || 'ws://localhost:8090';
    } catch {
      return 'ws://localhost:8090';
    }
  });
  const [connectionStatus, setConnectionStatus] = useState<ConnectionStatus>(
    connection?.mode === 'websocket' ? 'connecting' : 'connected',
  );
  const [connectionError, setConnectionError] = useState<string | null>(null);
  const [activeExecutor, setActiveExecutor] = useState<Executor>(executor);
  const [transactionType, setTransactionType] = useState<TransactionType>(transactionTypes[0] ?? 'query');
  const [showConnectionPanel, setShowConnectionPanel] = useState(false);
  const wsClientRef = useRef<{ disconnect(): void } | null>(null);
  const ownsClientRef = useRef(true);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const autoRunFired = useRef(false);
  const autoRunCode = useRef<string | null>(null);

  useEffect(() => {
    if (connectionLocked) return;
    try {
      localStorage.setItem(WS_URL_STORAGE_KEY, wsUrl);
    } catch {
    }
  }, [wsUrl, connectionLocked]);

  useEffect(() => {
    if (connectionMode === 'wasm') {
      setActiveExecutor(executor);
    }
  }, [executor, connectionMode]);

  useEffect(() => {
    if (!connectionLocked || connection?.mode !== 'websocket') return;

    let cancelled = false;
    let backoff = 1000;
    const maxBackoff = 30000;

    async function connect() {
      if (cancelled) return;
      setConnectionStatus('connecting');
      setConnectionError(null);

      try {
        if (ownsClientRef.current && wsClientRef.current) {
          wsClientRef.current.disconnect();
          wsClientRef.current = null;
        }

        let client: { disconnect(): void } & WsClient;
        if (customConnect) {
          client = await customConnect(lockedWsUrl ?? '') as unknown as { disconnect(): void } & WsClient;
          ownsClientRef.current = false;
        } else {
          client = await Client.connectWs(lockedWsUrl!, { timeoutMs: 30_000 }) as unknown as { disconnect(): void } & WsClient;
          ownsClientRef.current = true;
          if (cancelled) {
            client.disconnect();
            return;
          }
        }

        wsClientRef.current = client;
        const wsExecutor = new WsExecutor(client);
        wsExecutor.transactionType = transactionType;
        setActiveExecutor(wsExecutor);
        setConnectionStatus('connected');
        backoff = 1000;
      } catch (err) {
        if (cancelled) return;
        setConnectionStatus('error');
        setConnectionError(err instanceof Error ? err.message : String(err));
        if (!customConnect) {
          reconnectTimerRef.current = setTimeout(() => {
            connect();
          }, backoff);
          backoff = Math.min(backoff * 2, maxBackoff);
        }
      }
    }

    connect();

    return () => {
      cancelled = true;
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
      if (ownsClientRef.current && wsClientRef.current) {
        wsClientRef.current.disconnect();
        wsClientRef.current = null;
      }
    };
  }, [lockedWsUrl, customConnect]);

  useEffect(() => {
    const entries = loadHistory(historyKey);
    if (entries.length > 0) {
      dispatch({ type: 'LOAD_HISTORY', entries });
    }
  }, [historyKey, dispatch]);

  useEffect(() => {
    saveHistory(state.history, historyKey);
  }, [state.history, historyKey]);

  useEffect(() => {
    if (code === undefined) return;
    dispatch({ type: 'LOAD_QUERY', code });
  }, [code, dispatch]);

  useEffect(() => {
    if (catalogPlacement === 'sidebar' && state.activeTab === 'catalog') {
      dispatch({ type: 'SET_TAB', tab: 'results' });
    }
  }, [catalogPlacement, state.activeTab, dispatch]);

  const handleConnect = useCallback(async () => {
    if (!wsUrl.trim()) return;
    setConnectionStatus('connecting');
    setConnectionError(null);

    try {
      if (wsClientRef.current) {
        wsClientRef.current.disconnect();
        wsClientRef.current = null;
      }

      const client = await Client.connectWs(wsUrl, { timeoutMs: 30_000 });
      wsClientRef.current = client;
      const wsExecutor = new WsExecutor(client as unknown as WsClient);
      wsExecutor.transactionType = transactionType;
      setActiveExecutor(wsExecutor);
      setConnectionStatus('connected');
    } catch (err) {
      setConnectionStatus('error');
      setConnectionError(err instanceof Error ? err.message : String(err));
    }
  }, [wsUrl, transactionType]);

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

  const handleTransactionTypeChange = useCallback((type: TransactionType) => {
    setTransactionType(type);
    if (activeExecutor instanceof WsExecutor) {
      activeExecutor.transactionType = type;
    }
  }, [activeExecutor]);

  const handleModeChange = useCallback((mode: ConnectionMode) => {
    if (mode === 'wasm' && connectionMode === 'websocket') {
      if (wsClientRef.current) {
        wsClientRef.current.disconnect();
        wsClientRef.current = null;
      }
      setActiveExecutor(executor);
      setConnectionStatus('connected');
      setConnectionError(null);
    } else if (mode === 'websocket' && connectionMode === 'wasm') {
      setConnectionStatus('disconnected');
      setConnectionError(null);
    }
    setConnectionMode(mode);
  }, [connectionMode, executor]);

  const resolvedMonacoThemeName = useMemo(() => {
    if (!monacoTheme) return undefined;
    if (typeof monacoTheme === 'string') return monacoTheme;
    return 'rdb-custom';
  }, [monacoTheme]);

  const resolvedMonacoThemeData = useMemo(() => {
    if (!monacoTheme || typeof monacoTheme === 'string') return undefined;
    return monacoTheme;
  }, [monacoTheme]);

  const connectionLabel = connectionMode === 'wasm' ? 'wasm' : wsUrl;

  const handleRun = useCallback(async (override?: string) => {
    const rql = override ?? state.code;
    if (state.isExecuting || !rql.trim()) return;
    dispatch({ type: 'EXECUTE_START' });

    try {
      const result = await activeExecutor.execute(rql);
      if (result.success) {
        dispatch({ type: 'EXECUTE_SUCCESS', result, query: rql });
      } else {
        dispatch({ type: 'EXECUTE_ERROR', result, query: rql });
      }
    } catch (err) {
      dispatch({
        type: 'EXECUTE_ERROR',
        result: {
          success: false,
          error: err instanceof Error ? err.message : String(err),
          executionTime: 0,
        },
        query: rql,
      });
    }

  }, [state.isExecuting, state.code, activeExecutor, dispatch]);

  useEffect(() => {
    if (!autoRun) return;

    if (code !== undefined) {
      if (!code.trim() || autoRunCode.current === code) return;
      autoRunCode.current = code;
      handleRun(code);
      return;
    }

    if (autoRunFired.current || !state.code.trim()) return;
    autoRunFired.current = true;
    handleRun(state.code);
  }, [autoRun, code, state.code, handleRun]);

  const runCurrent = useCallback(() => {
    handleRun();
  }, [handleRun]);

  const handleCatalogSelect = useCallback((selected: string) => {
    dispatch({ type: 'LOAD_QUERY', code: selected });
    handleRun(selected);
  }, [dispatch, handleRun]);

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
          onRun={runCurrent}
          onClear={handleClear}
          isExecuting={state.isExecuting}
          connectionLabel={connectionLabel}
          connectionStatus={connectionStatus}
          connectionLocked={connectionLocked}
          onToggleConnectionPanel={() => setShowConnectionPanel((v) => !v)}
          connectionMode={connectionMode}
          transactionType={transactionType}
          transactionTypes={transactionTypes}
          onTransactionTypeChange={handleTransactionTypeChange}
        />
        {!connectionLocked && showConnectionPanel && (
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
          onRun={runCurrent}
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
        tabs={catalogPlacement === 'sidebar' ? TABS_WITH_SIDEBAR : TABS}
        onTabChange={(tab) => dispatch({ type: 'SET_TAB', tab: tab as 'results' | 'history' | 'catalog' })}
      />
      <div style={{ flex: 1, overflow: 'auto', minHeight: 0 }}>
        {state.activeTab === 'results' ? (
          <ResultsPanel result={state.result} />
        ) : state.activeTab === 'history' ? (
          <HistoryPanel entries={state.history} onSelect={handleSelectHistory} />
        ) : state.activeTab === 'catalog' && catalogPlacement === 'tab' ? (
          <CatalogBrowser executor={activeExecutor} namespaces={catalogNamespaces} onSelect={handleCatalogSelect} />
        ) : null}
      </div>
    </div>
  );

  return (
    <div className={`rdb-console${themeClass(theme)}`}>
      {catalogPlacement === 'sidebar' && (
        <aside className="rdb-console__sidebar">
          <div className="rdb-console__sidebar-title">Catalog</div>
          <div className="rdb-console__sidebar-body">
            <CatalogBrowser executor={activeExecutor} namespaces={catalogNamespaces} expandedPaths={catalogExpanded} onSelect={handleCatalogSelect} />
          </div>
        </aside>
      )}
      <div className="rdb-console__main">
        <SplitPane top={editorPane} bottom={bottomPane} initialSplit={45} />
      </div>
    </div>
  );
}

export function Console({ executor, initialCode, code, autoRun, historyKey, connection, theme, monacoTheme, transactionTypes, catalogPlacement, catalogExpanded, catalogNamespaces }: ConsoleProps) {
  return (
    <ConsoleProvider initialCode={code ?? initialCode}>
      <ConsoleInner executor={executor} code={code} autoRun={autoRun} historyKey={historyKey} connection={connection} theme={theme} monacoTheme={monacoTheme} transactionTypes={transactionTypes} catalogPlacement={catalogPlacement} catalogExpanded={catalogExpanded} catalogNamespaces={catalogNamespaces} />
    </ConsoleProvider>
  );
}
