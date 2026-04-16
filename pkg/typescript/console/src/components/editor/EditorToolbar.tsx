// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { ConnectionStatus } from '../connection/ConnectionPanel';
import type { TransactionType } from '../../types';

interface EditorToolbarProps {
  onRun: () => void;
  onClear: () => void;
  is_executing: boolean;
  connectionLabel: string;
  connection_status: ConnectionStatus;
  connection_locked?: boolean;
  onToggleConnectionPanel: () => void;
  connection_mode: 'wasm' | 'websocket';
  transaction_type: TransactionType;
  transaction_types: readonly TransactionType[];
  onTransactionTypeChange: (type: TransactionType) => void;
}

export function EditorToolbar({
  onRun,
  onClear,
  is_executing,
  connectionLabel,
  connection_status,
  connection_locked,
  onToggleConnectionPanel,
  connection_mode,
  transaction_type,
  transaction_types,
  onTransactionTypeChange,
}: EditorToolbarProps) {
  const show_tx_selector = connection_mode === 'websocket' && transaction_types.length > 1;
  return (
    <div className="rdb-editor-toolbar">
      <div className="rdb-editor-toolbar__left">
        {connection_locked ? (
          <span className="rdb-editor-toolbar__connection rdb-editor-toolbar__connection--locked">
            <span className={`rdb-editor-toolbar__connection-dot rdb-editor-toolbar__connection-dot--${connection_status}`}>●</span>
            <span>[{connectionLabel}]</span>
          </span>
        ) : (
          <button
            className="rdb-editor-toolbar__connection"
            onClick={onToggleConnectionPanel}
          >
            <span className={`rdb-editor-toolbar__connection-dot rdb-editor-toolbar__connection-dot--${connection_status}`}>●</span>
            <span>[{connectionLabel}]</span>
          </button>
        )}
        {show_tx_selector && (
          <div className="rdb-editor-toolbar__tx-type">
            {transaction_types.map((t) => (
              <button
                key={t}
                className={`rdb-editor-toolbar__tx-type-btn${t === transaction_type ? ' rdb-editor-toolbar__tx-type-btn--active' : ''}`}
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
          disabled={is_executing}
        >
          Clear
        </button>
        <button
          className="rdb-editor-toolbar__btn"
          onClick={onRun}
          disabled={is_executing}
        >
          {is_executing ? 'Running...' : 'Run'}
        </button>
      </div>
    </div>
  );
}
