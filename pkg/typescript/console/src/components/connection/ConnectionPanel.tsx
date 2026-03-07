// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useEffect, useRef } from 'react';

export type ConnectionMode = 'wasm' | 'websocket';
export type ConnectionStatus = 'connected' | 'connecting' | 'disconnected' | 'error';

export interface ConnectionPanelProps {
  mode: ConnectionMode;
  wsUrl: string;
  status: ConnectionStatus;
  error: string | null;
  onModeChange: (mode: ConnectionMode) => void;
  onUrlChange: (url: string) => void;
  onConnect: () => void;
  onDisconnect: () => void;
  onClose: () => void;
}

export function ConnectionPanel({
  mode,
  wsUrl,
  status,
  error,
  onModeChange,
  onUrlChange,
  onConnect,
  onDisconnect,
  onClose,
}: ConnectionPanelProps) {
  const panelRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (panelRef.current && !panelRef.current.contains(e.target as Node)) {
        onClose();
      }
    }
    function handleEscape(e: KeyboardEvent) {
      if (e.key === 'Escape') onClose();
    }
    document.addEventListener('mousedown', handleClickOutside);
    document.addEventListener('keydown', handleEscape);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
      document.removeEventListener('keydown', handleEscape);
    };
  }, [onClose]);

  const isWsConnected = mode === 'websocket' && status === 'connected';
  const isWsConnecting = mode === 'websocket' && status === 'connecting';

  return (
    <div className="rdb-connection-panel" ref={panelRef}>
      <div className="rdb-connection-panel__header">connection</div>

      <div className="rdb-connection-panel__modes">
        <button
          className={`rdb-connection-panel__mode-btn ${mode === 'wasm' ? 'rdb-connection-panel__mode-btn--active' : ''}`}
          onClick={() => onModeChange('wasm')}
        >
          [wasm (in-browser)]
        </button>
        <button
          className={`rdb-connection-panel__mode-btn ${mode === 'websocket' ? 'rdb-connection-panel__mode-btn--active' : ''}`}
          onClick={() => onModeChange('websocket')}
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
              value={wsUrl}
              onChange={(e) => onUrlChange(e.target.value)}
              placeholder="ws://localhost:8090"
              disabled={isWsConnected || isWsConnecting}
            />
          </div>
          <div className="rdb-connection-panel__actions">
            {isWsConnected ? (
              <button className="rdb-connection-panel__action-btn" onClick={onDisconnect}>
                [disconnect]
              </button>
            ) : (
              <button
                className="rdb-connection-panel__action-btn"
                onClick={onConnect}
                disabled={isWsConnecting || !wsUrl.trim()}
              >
                {isWsConnecting ? '[connecting...]' : '[connect]'}
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
