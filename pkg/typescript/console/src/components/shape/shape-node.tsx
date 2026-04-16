// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useState, type ReactNode } from 'react';

interface ShapeNodeProps {
  label: string;
  label_class?: string;
  type?: string;
  type_class?: string;
  children?: ReactNode;
}

export function ShapeNode({ label, label_class, type, type_class, children }: ShapeNodeProps) {
  const [expanded, setExpanded] = useState(false);
  const has_children = !!children;

  return (
    <div className="rdb-shape__node">
      <div
        className="rdb-shape__node-header"
        onClick={() => has_children && setExpanded(!expanded)}
      >
        {has_children && (
          <span className="rdb-shape__node-toggle">
            {expanded ? '▾' : '▸'}
          </span>
        )}
        {!has_children && <span className="rdb-shape__node-toggle" />}
        {type && <span className={type_class ? "rdb-shape__node-type " + type_class : "rdb-shape__node-type"}>{type}</span>}
        <span className={`rdb-shape__node-label${label_class ? ` ${label_class}` : ''}`}>
          {label}
        </span>
      </div>
      {expanded && has_children && (
        <div className="rdb-shape__node-children">
          {children}
        </div>
      )}
    </div>
  );
}
