// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect, useRef } from 'react'
import { Circle, Hand, Image, Pencil, Pipette, Slash, Square, type LucideIcon } from 'lucide-react'
import type { Tool } from '@/tools'

const TOOLS: { tool: Tool; label: string; key: string; icon: LucideIcon }[] = [
  { tool: 'pan', label: 'Pan', key: 'h', icon: Hand },
  { tool: 'pencil', label: 'Pencil', key: 'p', icon: Pencil },
  { tool: 'line', label: 'Line', key: 'l', icon: Slash },
  { tool: 'rect', label: 'Rectangle', key: 'r', icon: Square },
  { tool: 'ellipse', label: 'Ellipse', key: 'e', icon: Circle },
  { tool: 'eyedropper', label: 'Eyedropper', key: 'i', icon: Pipette },
  { tool: 'image', label: 'Image', key: 'u', icon: Image },
]

export function Toolbar({
  tool,
  locked,
  onSelect,
  onFile,
}: {
  tool: Tool
  locked: boolean
  onSelect: (tool: Tool) => void
  onFile: (file: File) => void
}) {
  const inputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.ctrlKey || event.metaKey || event.altKey || isTyping(event.target)) return
      const match = TOOLS.find((entry) => entry.key === event.key.toLowerCase())
      if (!match || (locked && match.tool !== 'pan')) return
      onSelect(match.tool)
      if (match.tool === 'image') inputRef.current?.click()
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [onSelect, locked])

  return (
    <div
      className="card absolute left-4 top-1/2 z-10 flex -translate-y-1/2 flex-col gap-1 p-1"
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
          disabled={locked && entry !== 'pan'}
          title={`${label} (${key.toUpperCase()})`}
          onClick={() => {
            onSelect(entry)
            if (entry === 'image') inputRef.current?.click()
          }}
          className={`flex h-10 w-10 items-center justify-center outline-none ${
            entry === tool ? 'bg-text-primary text-bg-primary' : 'text-text-primary hover:bg-bg-tertiary'
          } disabled:cursor-not-allowed disabled:opacity-30 disabled:hover:bg-transparent`}
        >
          <Icon size={18} />
        </button>
      ))}
      <input
        ref={inputRef}
        type="file"
        accept="image/*"
        aria-label="Image file"
        hidden
        onChange={(event) => {
          const file = event.target.files?.[0]
          event.target.value = ''
          if (file) onFile(file)
        }}
      />
    </div>
  )
}

export function isTyping(target: EventTarget | null): boolean {
  return target instanceof HTMLElement && (target.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(target.tagName))
}
