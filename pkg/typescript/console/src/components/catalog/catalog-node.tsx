// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useState, type ReactNode } from 'react';

interface CatalogNodeProps {
  label: string;
  label_class?: string;
  type?: string;
  type_class?: string;
  on_click?: () => void;
  children?: ReactNode;
}

export function CatalogNode({ label, label_class, type, type_class, on_click, children }: CatalogNodeProps) {
  const [expanded, setExpanded] = useState(false);
  const has_children = !!children;

  return (
    <div className="rdb-catalog__node">
      <div
        className="rdb-catalog__node-header"
        onClick={() => has_children && setExpanded(!expanded)}
      >
        {has_children && (
          <span className="rdb-catalog__node-toggle">
            {expanded ? '▾' : '▸'}
          </span>
        )}
        {!has_children && <span className="rdb-catalog__node-toggle" />}
        {type && <span className={type_class ? "rdb-catalog__node-type " + type_class : "rdb-catalog__node-type"}>{type}</span>}
        <span
          className={`rdb-catalog__node-label${label_class ? ` ${label_class}` : ''}${on_click ? ' rdb-catalog__node-label--clickable' : ''}`}
          onClick={on_click ? (e) => { e.stopPropagation(); on_click(); } : undefined}
        >
          {label}
        </span>
      </div>
      {expanded && has_children && (
        <div className="rdb-catalog__node-children">
          {children}
        </div>
      )}
    </div>
  );
}
