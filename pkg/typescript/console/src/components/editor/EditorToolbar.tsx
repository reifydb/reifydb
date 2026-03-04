// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

import type { ConnectionStatus } from '../connection/ConnectionPanel';

interface EditorToolbarProps {
  onRun: () => void;
  onClear: () => void;
  isExecuting: boolean;
  connectionLabel: string;
  connectionStatus: ConnectionStatus;
  connectionLocked?: boolean;
  onToggleConnectionPanel: () => void;
}

export function EditorToolbar({
  onRun,
  onClear,
  isExecuting,
  connectionLabel,
  connectionStatus,
  connectionLocked,
  onToggleConnectionPanel,
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
        <span className="rdb-editor-toolbar__hint">ctrl+enter to run</span>
      </div>
      <div className="rdb-editor-toolbar__actions">
        <button
          className="rdb-editor-toolbar__btn rdb-editor-toolbar__btn--secondary"
          onClick={onClear}
          disabled={isExecuting}
        >
          [clear]
        </button>
        <button
          className="rdb-editor-toolbar__btn"
          onClick={onRun}
          disabled={isExecuting}
        >
          {isExecuting ? '[running...]' : '[run]'}
        </button>
      </div>
    </div>
  );
}
