// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { PALETTE, cssColor } from '@/palette'

export function PalettePicker({ selected, onSelect }: { selected: number; onSelect: (index: number) => void }) {
  return (
    <div className="grid grid-cols-8 gap-px border-2 border-border-default bg-border-default" role="listbox" aria-label="Palette">
      {PALETTE.map((_, index) => (
        <button
          key={index}
          type="button"
          role="option"
          aria-selected={index === selected}
          aria-label={`Color ${index}`}
          title={`${index} ${cssColor(index)}`}
          onClick={() => onSelect(index)}
          className={`aspect-square w-full outline-none ${
            index === selected ? 'relative z-10 ring-2 ring-primary-dark ring-offset-1' : 'hover:opacity-80'
          }`}
          style={{ backgroundColor: cssColor(index) }}
        />
      ))}
    </div>
  )
}
