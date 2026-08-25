// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useState, type ReactNode } from 'react';

interface CatalogNodeProps {
  label: string;
  label_class?: string;
  type?: string;
  type_class?: string;
  on_click?: () => void;
  default_expanded?: boolean;
  children?: ReactNode;
}

export function CatalogNode({ label, label_class, type, type_class, on_click, default_expanded, children }: CatalogNodeProps) {
  const [expanded, setExpanded] = useState(default_expanded ?? false);
  const has_children = !!children;
  const toggle = () => setExpanded(!expanded);

  return (
    <div className="rdb-catalog__node">
      <div
        className={`rdb-catalog__node-header${on_click ? ' rdb-catalog__node-header--clickable' : ''}`}
        onClick={on_click ?? (has_children ? toggle : undefined)}
      >
        {has_children ? (
          <span
            className="rdb-catalog__node-toggle"
            onClick={on_click ? (e) => { e.stopPropagation(); toggle(); } : undefined}
          >
            {expanded ? '▾' : '▸'}
          </span>
        ) : (
          <span className="rdb-catalog__node-toggle" />
        )}
        {type && <span className={type_class ? "rdb-catalog__node-type " + type_class : "rdb-catalog__node-type"}>{type}</span>}
        <span className={`rdb-catalog__node-label${label_class ? ` ${label_class}` : ''}`}>
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
