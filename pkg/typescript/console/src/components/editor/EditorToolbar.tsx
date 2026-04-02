// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { ConnectionStatus } from '../connection/ConnectionPanel';
import type { TransactionType } from '../../types';

const TX_TYPES: TransactionType[] = ['query', 'command', 'admin'];

interface EditorToolbarProps {
  onRun: () => void;
  onClear: () => void;
  isExecuting: boolean;
  connectionLabel: string;
  connectionStatus: ConnectionStatus;
  connectionLocked?: boolean;
  onToggleConnectionPanel: () => void;
  connectionMode: 'wasm' | 'websocket';
  transactionType: TransactionType;
  onTransactionTypeChange: (type: TransactionType) => void;
}

export function EditorToolbar({
  onRun,
  onClear,
  isExecuting,
  connectionLabel,
  connectionStatus,
  connectionLocked,
  onToggleConnectionPanel,
  connectionMode,
  transactionType,
  onTransactionTypeChange,
}: EditorToolbarProps) {
  return (
    <div className="rdb-editor-toolbar">
      <div className="rdb-editor-toolbar__left">
        {connectionLocked ? (
          <span className="rdb-editor-toolbar__connection rdb-editor-toolbar__connection--locked">
            <span className={`rdb-editor-toolbar__connection-dot rdb-editor-toolbar__connection-dot--${connectionStatus}`}>●</span>
            <span>[{connectionLabel}]</span>
          </span>
        ) : (
          <button
            className="rdb-editor-toolbar__connection"
            onClick={onToggleConnectionPanel}
          >
            <span className={`rdb-editor-toolbar__connection-dot rdb-editor-toolbar__connection-dot--${connectionStatus}`}>●</span>
            <span>[{connectionLabel}]</span>
          </button>
        )}
        {connectionMode === 'websocket' && (
          <div className="rdb-editor-toolbar__tx-type">
            {TX_TYPES.map((t) => (
              <button
                key={t}
                className={`rdb-editor-toolbar__tx-type-btn${t === transactionType ? ' rdb-editor-toolbar__tx-type-btn--active' : ''}`}
                onClick={() => onTransactionTypeChange(t)}
              >
                {t}
              </button>
            ))}
          </div>
        )}
        <span className="rdb-editor-toolbar__hint">ctrl+enter to run</span>
      </div>
      <div className="rdb-editor-toolbar__actions">
        <button
          className="rdb-editor-toolbar__btn rdb-editor-toolbar__btn--secondary"
          onClick={onClear}
          disabled={isExecuting}
        >
          Clear
        </button>
        <button
          className="rdb-editor-toolbar__btn"
          onClick={onRun}
          disabled={isExecuting}
        >
          {isExecuting ? 'Running...' : 'Run'}
        </button>
      </div>
    </div>
  );
}
