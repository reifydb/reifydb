// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useState, type ReactNode } from 'react';

interface ShapeNodeProps {
  label: string;
  labelClass?: string;
  type?: string;
  typeClass?: string;
  children?: ReactNode;
}

export function ShapeNode({ label, labelClass, type, typeClass, children }: ShapeNodeProps) {
  const [expanded, setExpanded] = useState(false);
  const hasChildren = !!children;

  return (
    <div className="rdb-shape__node">
      <div
        className="rdb-shape__node-header"
        onClick={() => hasChildren && setExpanded(!expanded)}
      >
        {hasChildren && (
          <span className="rdb-shape__node-toggle">
            {expanded ? '▾' : '▸'}
          </span>
        )}
        {!hasChildren && <span className="rdb-shape__node-toggle" />}
        {type && <span className={typeClass ? "rdb-shape__node-type " + typeClass : "rdb-shape__node-type"}>{type}</span>}
        <span className={`rdb-shape__node-label${labelClass ? ` ${labelClass}` : ''}`}>
          {label}
        </span>
      </div>
      {expanded && hasChildren && (
        <div className="rdb-shape__node-children">
          {children}
        </div>
      )}
    </div>
  );
}
