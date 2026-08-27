// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useState, type ReactNode } from 'react';

interface CatalogNodeProps {
  label: string;
  labelClass?: string;
  type?: string;
  typeClass?: string;
  onClick?: () => void;
  children?: ReactNode;
}

export function CatalogNode({ label, labelClass, type, typeClass, onClick, children }: CatalogNodeProps) {
  const [expanded, setExpanded] = useState(false);
  const hasChildren = !!children;

  return (
    <div className="rdb-catalog__node">
      <div
        className="rdb-catalog__node-header"
        onClick={() => hasChildren && setExpanded(!expanded)}
      >
        {hasChildren && (
          <span className="rdb-catalog__node-toggle">
            {expanded ? '▾' : '▸'}
          </span>
        )}
        {!hasChildren && <span className="rdb-catalog__node-toggle" />}
        {type && <span className={typeClass ? "rdb-catalog__node-type " + typeClass : "rdb-catalog__node-type"}>{type}</span>}
        <span
          className={`rdb-catalog__node-label${labelClass ? ` ${labelClass}` : ''}${onClick ? ' rdb-catalog__node-label--clickable' : ''}`}
          onClick={onClick ? (e) => { e.stopPropagation(); onClick(); } : undefined}
        >
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
