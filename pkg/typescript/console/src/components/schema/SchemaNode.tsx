// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useState, type ReactNode } from 'react';

interface SchemaNodeProps {
  label: string;
  labelClass?: string;
  type?: string;
  typeClass?: string;
  children?: ReactNode;
}

export function SchemaNode({ label, labelClass, type, typeClass, children }: SchemaNodeProps) {
  const [expanded, setExpanded] = useState(false);
  const hasChildren = !!children;

  return (
    <div className="rdb-schema__node">
      <div
        className="rdb-schema__node-header"
        onClick={() => hasChildren && setExpanded(!expanded)}
      >
        {hasChildren && (
          <span className="rdb-schema__node-toggle">
            {expanded ? '▾' : '▸'}
          </span>
        )}
        {!hasChildren && <span className="rdb-schema__node-toggle" />}
        {type && <span className={typeClass ? "rdb-schema__node-type " + typeClass : "rdb-schema__node-type"}>{type}</span>}
        <span className={`rdb-schema__node-label${labelClass ? ` ${labelClass}` : ''}`}>
          {label}
        </span>
      </div>
      {expanded && hasChildren && (
        <div className="rdb-schema__node-children">
          {children}
        </div>
      )}
    </div>
  );
}
