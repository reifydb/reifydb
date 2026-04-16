// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { ConnectionStatus } from '../connection/connection-panel';
import type { TransactionType } from '../../types';

interface EditorToolbarProps {
  on_run: () => void;
  on_clear: () => void;
  is_executing: boolean;
  connection_label: string;
  connection_status: ConnectionStatus;
  connection_locked?: boolean;
  on_toggle_connection_panel: () => void;
  connection_mode: 'wasm' | 'websocket';
  transaction_type: TransactionType;
  transaction_types: readonly TransactionType[];
  on_transaction_type_change: (type: TransactionType) => void;
}

export function EditorToolbar({
  on_run,
  on_clear,
  is_executing,
  connection_label,
  connection_status,
  connection_locked,
  on_toggle_connection_panel,
  connection_mode,
  transaction_type,
  transaction_types,
  on_transaction_type_change,
}: EditorToolbarProps) {
  const show_tx_selector = connection_mode === 'websocket' && transaction_types.length > 1;
  return (
    <div className="rdb-editor-toolbar">
      <div className="rdb-editor-toolbar__left">
        {connection_locked ? (
          <span className="rdb-editor-toolbar__connection rdb-editor-toolbar__connection--locked">
            <span className={`rdb-editor-toolbar__connection-dot rdb-editor-toolbar__connection-dot--${connection_status}`}>●</span>
            <span>[{connection_label}]</span>
          </span>
        ) : (
          <button
            className="rdb-editor-toolbar__connection"
            onClick={on_toggle_connection_panel}
          >
            <span className={`rdb-editor-toolbar__connection-dot rdb-editor-toolbar__connection-dot--${connection_status}`}>●</span>
            <span>[{connection_label}]</span>
          </button>
        )}
        {show_tx_selector && (
          <div className="rdb-editor-toolbar__tx-type">
            {transaction_types.map((t) => (
              <button
                key={t}
                className={`rdb-editor-toolbar__tx-type-btn${t === transaction_type ? ' rdb-editor-toolbar__tx-type-btn--active' : ''}`}
                onClick={() => on_transaction_type_change(t)}
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
          onClick={on_clear}
          disabled={is_executing}
        >
          Clear
        </button>
        <button
          className="rdb-editor-toolbar__btn"
          onClick={on_run}
          disabled={is_executing}
        >
          {is_executing ? 'Running...' : 'Run'}
        </button>
      </div>
    </div>
  );
}
