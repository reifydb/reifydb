// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect } from 'react'
import { Circle, Eraser, Hand, Pencil, Pipette, Slash, Square, type LucideIcon } from 'lucide-react'
import type { Tool } from '@/tools'

const TOOLS: { tool: Tool; label: string; key: string; icon: LucideIcon }[] = [
  { tool: 'pan', label: 'Pan', key: 'h', icon: Hand },
  { tool: 'pencil', label: 'Pencil', key: 'p', icon: Pencil },
  { tool: 'line', label: 'Line', key: 'l', icon: Slash },
  { tool: 'rect', label: 'Rectangle', key: 'r', icon: Square },
  { tool: 'ellipse', label: 'Ellipse', key: 'e', icon: Circle },
  { tool: 'eraser', label: 'Eraser', key: 'x', icon: Eraser },
  { tool: 'eyedropper', label: 'Eyedropper', key: 'i', icon: Pipette },
]

export function Toolbar({ tool, onSelect }: { tool: Tool; onSelect: (tool: Tool) => void }) {
  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.ctrlKey || event.metaKey || event.altKey || isTyping(event.target)) return
      const match = TOOLS.find((entry) => entry.key === event.key.toLowerCase())
      if (match) onSelect(match.tool)
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [onSelect])

  return (
    <div
      className="absolute left-3 top-3 z-10 flex flex-col gap-px border-2 border-border-default bg-border-default"
      role="toolbar"
      aria-label="Tools"
      aria-orientation="vertical"
    >
      {TOOLS.map(({ tool: entry, label, key, icon: Icon }) => (
        <button
          key={entry}
          type="button"
          aria-label={label}
          aria-pressed={entry === tool}
          title={`${label} (${key.toUpperCase()})`}
          onClick={() => onSelect(entry)}
          className={`flex h-10 w-10 items-center justify-center outline-none ${
            entry === tool ? 'bg-primary-dark text-bg-primary' : 'bg-bg-primary text-text-primary hover:bg-bg-tertiary'
          }`}
        >
          <Icon size={18} />
        </button>
      ))}
    </div>
  )
}

export function isTyping(target: EventTarget | null): boolean {
  return target instanceof HTMLElement && (target.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(target.tagName))
}
