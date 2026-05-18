// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useState, useCallback, useRef, useEffect, type ReactNode } from 'react';

interface SplitPaneProps {
  top: ReactNode;
  bottom: ReactNode;
  initial_split?: number; // percentage for top pane, default 50
}

export function SplitPane({ top, bottom, initial_split = 50 }: SplitPaneProps) {
  const [split, setSplit] = useState(initial_split);
  const container_ref = useRef<HTMLDivElement>(null);
  const dragging = useRef(false);

  const on_mouse_down = useCallback(() => {
    dragging.current = true;
    document.body.style.cursor = 'row-resize';
    document.body.style.userSelect = 'none';
  }, []);

  useEffect(() => {
    const on_mouse_move = (e: MouseEvent) => {
      if (!dragging.current || !container_ref.current) return;
      const rect = container_ref.current.getBoundingClientRect();
      const pct = ((e.clientY - rect.top) / rect.height) * 100;
      setSplit(Math.min(Math.max(pct, 15), 85));
    };

    const on_mouse_up = () => {
      if (dragging.current) {
        dragging.current = false;
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
      }
    };

    document.addEventListener('mousemove', on_mouse_move);
    document.addEventListener('mouseup', on_mouse_up);
    return () => {
      document.removeEventListener('mousemove', on_mouse_move);
      document.removeEventListener('mouseup', on_mouse_up);
    };
  }, []);

  return (
    <div className="rdb-split" ref={container_ref}>
      <div className="rdb-split__top" style={{ height: `${split}%` }}>
        {top}
      </div>
      <div className="rdb-split__handle" onMouseDown={on_mouse_down} />
      <div className="rdb-split__bottom">
        {bottom}
      </div>
    </div>
  );
}
