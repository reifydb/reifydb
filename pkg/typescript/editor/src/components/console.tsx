// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Client } from '@reifydb/client';
import type { editor } from 'monaco-editor';
import type { Executor, RdbTheme, TransactionType } from '../types';
import { WsExecutor, type WsClient } from '../executor/ws-executor';
import { ConsoleProvider, useConsoleStore } from '../state/use-console-store';
import { load_history, save_history } from '../state/history';
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

export function theme_class(theme: RdbTheme = 'light'): string {
  return THEME_CLASSES[theme];
}

export type CatalogPlacement = 'tab' | 'sidebar';

export interface ConsoleProps {
  executor: Executor;
  initial_code?: string;
  code?: string;
  auto_run?: boolean;
  history_key?: string;
  connection?: ConnectionConfig;
  theme?: RdbTheme;
  monaco_theme?: string | editor.IStandaloneThemeData;
  transaction_types?: readonly TransactionType[];
  catalog_placement?: CatalogPlacement;
  catalog_expanded?: readonly string[];
  catalog_namespaces?: readonly string[];
}

const DEFAULT_TRANSACTION_TYPES: readonly TransactionType[] = ['query', 'command', 'admin'];

const TABS = [
  { id: 'results', label: 'Results' },
  { id: 'history', label: 'History' },
  { id: 'catalog', label: 'Catalog' },
];

const TABS_WITH_SIDEBAR = TABS.filter((tab) => tab.id !== 'catalog');

const WS_URL_STORAGE_KEY = 'rdb-console-ws-url';

function ConsoleInner({ executor, code, auto_run, history_key, connection, theme = 'light', monaco_theme, transaction_types = DEFAULT_TRANSACTION_TYPES, catalog_placement = 'tab', catalog_expanded, catalog_namespaces }: { executor: Executor; code?: string; auto_run?: boolean; history_key?: string; connection?: ConnectionConfig; theme?: RdbTheme; monaco_theme?: string | editor.IStandaloneThemeData; transaction_types?: readonly TransactionType[]; catalog_placement?: CatalogPlacement; catalog_expanded?: readonly string[]; catalog_namespaces?: readonly string[] }) {
  const { state, dispatch } = useConsoleStore();
  const connection_locked = connection != null;
  const locked_ws_url = connection?.mode === 'websocket' ? (connection.url ?? null) : null;
  const custom_connect = connection?.mode === 'websocket' ? (connection.connect ?? null) : null;

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
  const [transaction_type, set_transaction_type] = useState<TransactionType>(transaction_types[0] ?? 'query');
  const [show_connection_panel, set_show_connection_panel] = useState(false);
  const ws_client_ref = useRef<{ disconnect(): void } | null>(null);
  const owns_client_ref = useRef(true);
  const reconnect_timer_ref = useRef<ReturnType<typeof setTimeout> | null>(null);
  const auto_run_fired = useRef(false);
  const auto_run_code = useRef<string | null>(null);

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
    if (!connection_locked || connection?.mode !== 'websocket') return;

    let cancelled = false;
    let backoff = 1000;
    const max_backoff = 30000;

    async function connect() {
      if (cancelled) return;
      set_connection_status('connecting');
      set_connection_error(null);

      try {
        if (owns_client_ref.current && ws_client_ref.current) {
          ws_client_ref.current.disconnect();
          ws_client_ref.current = null;
        }

        let client: { disconnect(): void } & WsClient;
        if (custom_connect) {
          client = await custom_connect(locked_ws_url ?? '') as unknown as { disconnect(): void } & WsClient;
          owns_client_ref.current = false;
        } else {
          client = await Client.connect_ws(locked_ws_url!, { timeout_ms: 30_000 }) as unknown as { disconnect(): void } & WsClient;
          owns_client_ref.current = true;
          if (cancelled) {
            client.disconnect();
            return;
          }
        }

        ws_client_ref.current = client;
        const ws_executor = new WsExecutor(client);
        ws_executor.transaction_type = transaction_type;
        set_active_executor(ws_executor);
        set_connection_status('connected');
        backoff = 1000;
      } catch (err) {
        if (cancelled) return;
        set_connection_status('error');
        set_connection_error(err instanceof Error ? err.message : String(err));
        // Only retry when Console owns the connection; consumer handles reconnect otherwise
        if (!custom_connect) {
          reconnect_timer_ref.current = setTimeout(() => {
            connect();
          }, backoff);
          backoff = Math.min(backoff * 2, max_backoff);
        }
      }
    }

    connect();

    return () => {
      cancelled = true;
      if (reconnect_timer_ref.current) {
        clearTimeout(reconnect_timer_ref.current);
        reconnect_timer_ref.current = null;
      }
      if (owns_client_ref.current && ws_client_ref.current) {
        ws_client_ref.current.disconnect();
        ws_client_ref.current = null;
      }
    };
  }, [locked_ws_url, custom_connect]);

  // Load history on mount
  useEffect(() => {
    const entries = load_history(history_key);
    if (entries.length > 0) {
      dispatch({ type: 'LOAD_HISTORY', entries });
    }
  }, [history_key, dispatch]);

  // Save history on change
  useEffect(() => {
    save_history(state.history, history_key);
  }, [state.history, history_key]);

  useEffect(() => {
    if (code === undefined) return;
    dispatch({ type: 'LOAD_QUERY', code });
  }, [code, dispatch]);

  useEffect(() => {
    if (catalog_placement === 'sidebar' && state.active_tab === 'catalog') {
      dispatch({ type: 'SET_TAB', tab: 'results' });
    }
  }, [catalog_placement, state.active_tab, dispatch]);

  const handle_connect = useCallback(async () => {
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

  const handle_transaction_type_change = useCallback((type: TransactionType) => {
    set_transaction_type(type);
    if (active_executor instanceof WsExecutor) {
      active_executor.transaction_type = type;
    }
  }, [active_executor]);

  const handle_mode_change = useCallback((mode: ConnectionMode) => {
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

  const resolved_monaco_theme_name = useMemo(() => {
    if (!monaco_theme) return undefined;
    if (typeof monaco_theme === 'string') return monaco_theme;
    return 'rdb-custom';
  }, [monaco_theme]);

  const resolved_monaco_theme_data = useMemo(() => {
    if (!monaco_theme || typeof monaco_theme === 'string') return undefined;
    return monaco_theme;
  }, [monaco_theme]);

  const connection_label = connection_mode === 'wasm' ? 'wasm' : ws_url;

  const handle_run = useCallback(async (override?: string) => {
    const rql = override ?? state.code;
    if (state.is_executing || !rql.trim()) return;
    dispatch({ type: 'EXECUTE_START' });

    try {
      const result = await active_executor.execute(rql);
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
          execution_time: 0,
        },
        query: rql,
      });
    }

  }, [state.is_executing, state.code, active_executor, dispatch]);

  useEffect(() => {
    if (!auto_run) return;

    if (code !== undefined) {
      if (!code.trim() || auto_run_code.current === code) return;
      auto_run_code.current = code;
      handle_run(code);
      return;
    }

    if (auto_run_fired.current || !state.code.trim()) return;
    auto_run_fired.current = true;
    handle_run(state.code);
  }, [auto_run, code, state.code, handle_run]);

  const run_current = useCallback(() => {
    handle_run();
  }, [handle_run]);

  const handle_catalog_select = useCallback((selected: string) => {
    dispatch({ type: 'LOAD_QUERY', code: selected });
    handle_run(selected);
  }, [dispatch, handle_run]);

  const handle_clear = useCallback(() => {
    dispatch({ type: 'CLEAR_RESULTS' });
  }, [dispatch]);

  const handle_select_history = useCallback((query: string) => {
    dispatch({ type: 'LOAD_QUERY', code: query });
  }, [dispatch]);

  const editor_pane = (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      <div style={{ position: 'relative' }}>
        <EditorToolbar
          on_run={run_current}
          on_clear={handle_clear}
          is_executing={state.is_executing}
          connection_label={connection_label}
          connection_status={connection_status}
          connection_locked={connection_locked}
          on_toggle_connection_panel={() => set_show_connection_panel((v) => !v)}
          connection_mode={connection_mode}
          transaction_type={transaction_type}
          transaction_types={transaction_types}
          on_transaction_type_change={handle_transaction_type_change}
        />
        {!connection_locked && show_connection_panel && (
          <ConnectionPanel
            mode={connection_mode}
            ws_url={ws_url}
            status={connection_status}
            error={connection_error}
            on_mode_change={handle_mode_change}
            on_url_change={set_ws_url}
            on_connect={handle_connect}
            on_disconnect={handle_disconnect}
            on_close={() => set_show_connection_panel(false)}
          />
        )}
      </div>
      <div style={{ flex: 1, minHeight: 0 }}>
        <QueryEditor
          code={state.code}
          on_change={(code) => dispatch({ type: 'SET_CODE', code })}
          on_run={run_current}
          theme={theme}
          monaco_theme_name={resolved_monaco_theme_name}
          monaco_theme_data={resolved_monaco_theme_data}
        />
      </div>
    </div>
  );

  const bottom_pane = (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      <TabBar
        active_tab={state.active_tab}
        tabs={catalog_placement === 'sidebar' ? TABS_WITH_SIDEBAR : TABS}
        on_tab_change={(tab) => dispatch({ type: 'SET_TAB', tab: tab as 'results' | 'history' | 'catalog' })}
      />
      <div style={{ flex: 1, overflow: 'auto', minHeight: 0 }}>
        {state.active_tab === 'results' ? (
          <ResultsPanel result={state.result} />
        ) : state.active_tab === 'history' ? (
          <HistoryPanel entries={state.history} on_select={handle_select_history} />
        ) : state.active_tab === 'catalog' && catalog_placement === 'tab' ? (
          <CatalogBrowser executor={active_executor} namespaces={catalog_namespaces} on_select={handle_catalog_select} />
        ) : null}
      </div>
    </div>
  );

  return (
    <div className={`rdb-console${theme_class(theme)}`}>
      {catalog_placement === 'sidebar' && (
        <aside className="rdb-console__sidebar">
          <div className="rdb-console__sidebar-title">Catalog</div>
          <div className="rdb-console__sidebar-body">
            <CatalogBrowser executor={active_executor} namespaces={catalog_namespaces} expanded_paths={catalog_expanded} on_select={handle_catalog_select} />
          </div>
        </aside>
      )}
      <div className="rdb-console__main">
        <SplitPane top={editor_pane} bottom={bottom_pane} initial_split={45} />
      </div>
    </div>
  );
}

export function Console({ executor, initial_code, code, auto_run, history_key, connection, theme, monaco_theme, transaction_types, catalog_placement, catalog_expanded, catalog_namespaces }: ConsoleProps) {
  return (
    <ConsoleProvider initial_code={code ?? initial_code}>
      <ConsoleInner executor={executor} code={code} auto_run={auto_run} history_key={history_key} connection={connection} theme={theme} monaco_theme={monaco_theme} transaction_types={transaction_types} catalog_placement={catalog_placement} catalog_expanded={catalog_expanded} catalog_namespaces={catalog_namespaces} />
    </ConsoleProvider>
  );
}
