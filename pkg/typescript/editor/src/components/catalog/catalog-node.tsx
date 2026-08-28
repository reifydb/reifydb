// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useState, type ReactNode } from 'react';

interface CatalogNodeProps {
  label: string;
  labelClass?: string;
  type?: string;
  typeClass?: string;
  onClick?: () => void;
  defaultExpanded?: boolean;
  children?: ReactNode;
}

export function CatalogNode({ label, labelClass, type, typeClass, onClick, defaultExpanded, children }: CatalogNodeProps) {
  const [expanded, setExpanded] = useState(defaultExpanded ?? false);
  const hasChildren = !!children;
  const toggle = () => setExpanded(!expanded);

  return (
    <div className="rdb-catalog__node">
      <div
        className={`rdb-catalog__node-header${onClick ? ' rdb-catalog__node-header--clickable' : ''}`}
        onClick={onClick ?? (hasChildren ? toggle : undefined)}
      >
        {hasChildren ? (
          <span
            className="rdb-catalog__node-toggle"
            onClick={onClick ? (e) => { e.stopPropagation(); toggle(); } : undefined}
          >
            {expanded ? '▾' : '▸'}
          </span>
        ) : (
          <span className="rdb-catalog__node-toggle" />
        )}
        {type && <span className={typeClass ? "rdb-catalog__node-type " + typeClass : "rdb-catalog__node-type"}>{type}</span>}
        <span className={`rdb-catalog__node-label${labelClass ? ` ${labelClass}` : ''}`}>
          {label}
        </span>
      </div>
      {expanded && hasChildren && (
        <div className="rdb-catalog__node-children">
          {children}
        </div>
      )}
    </div>
  );
}
