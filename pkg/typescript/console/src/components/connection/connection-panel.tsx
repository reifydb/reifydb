// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useEffect, useRef } from 'react';

export type ConnectionMode = 'wasm' | 'websocket';
export type ConnectionStatus = 'connected' | 'connecting' | 'disconnected' | 'error';

export interface ConnectionPanelProps {
  mode: ConnectionMode;
  ws_url: string;
  status: ConnectionStatus;
  error: string | null;
  on_mode_change: (mode: ConnectionMode) => void;
  on_url_change: (url: string) => void;
  on_connect: () => void;
  on_disconnect: () => void;
  on_close: () => void;
}

export function ConnectionPanel({
  mode,
  ws_url,
  status,
  error,
  on_mode_change,
  on_url_change,
  on_connect,
  on_disconnect,
  on_close,
}: ConnectionPanelProps) {
  const panel_ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handle_click_outside(e: MouseEvent) {
      if (panel_ref.current && !panel_ref.current.contains(e.target as Node)) {
        on_close();
      }
    }
    function handle_escape(e: KeyboardEvent) {
      if (e.key === 'Escape') on_close();
    }
    document.addEventListener('mousedown', handle_click_outside);
    document.addEventListener('keydown', handle_escape);
    return () => {
      document.removeEventListener('mousedown', handle_click_outside);
      document.removeEventListener('keydown', handle_escape);
    };
  }, [on_close]);

  const is_ws_connected = mode === 'websocket' && status === 'connected';
  const is_ws_connecting = mode === 'websocket' && status === 'connecting';

  return (
    <div className="rdb-connection-panel" ref={panel_ref}>
      <div className="rdb-connection-panel__header">connection</div>

      <div className="rdb-connection-panel__modes">
        <button
          className={`rdb-connection-panel__mode-btn ${mode === 'wasm' ? 'rdb-connection-panel__mode-btn--active' : ''}`}
          onClick={() => on_mode_change('wasm')}
        >
          [wasm (in-browser)]
        </button>
        <button
          className={`rdb-connection-panel__mode-btn ${mode === 'websocket' ? 'rdb-connection-panel__mode-btn--active' : ''}`}
          onClick={() => on_mode_change('websocket')}
        >
          [websocket (remote)]
        </button>
      </div>

      {mode === 'websocket' && (
        <>
          <div className="rdb-connection-panel__url-row">
            <span className="rdb-connection-panel__url-label">url:</span>
            <input
              className="rdb-connection-panel__url-input"
              type="text"
              value={ws_url}
              onChange={(e) => on_url_change(e.target.value)}
              placeholder="ws://localhost:8090"
              disabled={is_ws_connected || is_ws_connecting}
            />
          </div>
          <div className="rdb-connection-panel__actions">
            {is_ws_connected ? (
              <button className="rdb-connection-panel__action-btn" onClick={on_disconnect}>
                [disconnect]
              </button>
            ) : (
              <button
                className="rdb-connection-panel__action-btn"
                onClick={on_connect}
                disabled={is_ws_connecting || !ws_url.trim()}
              >
                {is_ws_connecting ? '[connecting...]' : '[connect]'}
              </button>
            )}
          </div>
        </>
      )}

      <div className="rdb-connection-panel__status">
        <span className={`rdb-connection-panel__status-dot rdb-connection-panel__status-dot--${status}`}>●</span>
        <span>{status}</span>
      </div>

      {error && (
        <div className="rdb-connection-panel__error">{error}</div>
      )}
    </div>
  );
}
