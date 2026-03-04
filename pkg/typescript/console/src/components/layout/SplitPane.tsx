// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

import { useState, useCallback, useRef, useEffect, type ReactNode } from 'react';

interface SplitPaneProps {
  top: ReactNode;
  bottom: ReactNode;
  initialSplit?: number; // percentage for top pane, default 50
}

export function SplitPane({ top, bottom, initialSplit = 50 }: SplitPaneProps) {
  const [split, setSplit] = useState(initialSplit);
  const containerRef = useRef<HTMLDivElement>(null);
  const dragging = useRef(false);

  const onMouseDown = useCallback(() => {
    dragging.current = true;
    document.body.style.cursor = 'row-resize';
    document.body.style.userSelect = 'none';
  }, []);

  useEffect(() => {
    const onMouseMove = (e: MouseEvent) => {
      if (!dragging.current || !containerRef.current) return;
      const rect = containerRef.current.getBoundingClientRect();
      const pct = ((e.clientY - rect.top) / rect.height) * 100;
      setSplit(Math.min(Math.max(pct, 15), 85));
    };

    const onMouseUp = () => {
      if (dragging.current) {
        dragging.current = false;
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
      }
    };

    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
    return () => {
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
    };
  }, []);

  return (
    <div className="rdb-split" ref={containerRef}>
      <div className="rdb-split__top" style={{ height: `${split}%` }}>
        {top}
      </div>
      <div className="rdb-split__handle" onMouseDown={onMouseDown} />
      <div className="rdb-split__bottom">
        {bottom}
      </div>
    </div>
  );
}
