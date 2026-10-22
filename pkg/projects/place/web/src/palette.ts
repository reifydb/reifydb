// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

const SYSTEM = [
  0x000000, 0xcd0000, 0x00cd00, 0xcdcd00, 0x0000ee, 0xcd00cd, 0x00cdcd, 0xe5e5e5,
  0x7f7f7f, 0xff0000, 0x00ff00, 0xffff00, 0x5c5cff, 0xff00ff, 0x00ffff, 0xffffff,
]

const CUBE_LEVELS = [0, 95, 135, 175, 215, 255]

function xterm256(): number[] {
  const colors = [...SYSTEM]
  for (let r = 0; r < 6; r++) {
    for (let g = 0; g < 6; g++) {
      for (let b = 0; b < 6; b++) {
        colors.push((CUBE_LEVELS[r] << 16) | (CUBE_LEVELS[g] << 8) | CUBE_LEVELS[b])
      }
    }
  }
  for (let i = 0; i < 24; i++) {
    const level = 8 + 10 * i
    colors.push((level << 16) | (level << 8) | level)
  }
  ;[colors[0], colors[15]] = [colors[15], colors[0]]
  return colors
}

export const PALETTE: readonly number[] = xterm256()

export function cssColor(index: number): string {
  return `#${PALETTE[index].toString(16).padStart(6, '0')}`
}
