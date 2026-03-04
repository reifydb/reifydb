// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

interface EditorToolbarProps {
  onRun: () => void;
  onClear: () => void;
  isExecuting: boolean;
}

export function EditorToolbar({ onRun, onClear, isExecuting }: EditorToolbarProps) {
  return (
    <div className="rdb-editor-toolbar">
      <span className="rdb-editor-toolbar__hint">ctrl+enter to run</span>
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
