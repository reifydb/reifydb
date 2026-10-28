// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect, useState } from 'react'

const ROBOT = ['...a...', '...l...', '...l...', 'lwwwwwl', 'lwpwpwl', 'lwwwwwl', 'leeeeel', '.kk.kk.', '.kk.kk.']
const ROBOT_COLORS: Record<string, string> = { a: '#ffe08a', l: '#c9c3dc', w: '#ffffff', p: '#7e85f2', e: '#e6e4f5', k: '#3f3b99' }
const RING = ['..TTTTT..', '.L.TTT.R.', 'LL.....RR', 'LL.....RR', 'LL.....RR', 'LL.....RR', 'LL.....RR', '.L.BBB.R.', '..BBBBB..']
const RING_COLORS: Record<string, string> = { T: '#3d44c9', R: '#a8acf5', B: '#7e85f2', L: '#5d64e6' }
const CORE = '#262a8f'

function cells(rows: string[]) {
  return rows.flatMap((row, y) => [...row].map((cell, x) => ({ cell, x, y })).filter(({ cell }) => cell !== '.'))
}

export function Robot({ scale = 2, blinking = false }: { scale?: number; blinking?: boolean }) {
  return (
    <svg
      className={`inline-block shrink-0 align-middle ${blinking ? 'robot-blink' : ''}`}
      width={7 * scale}
      height={9 * scale}
      viewBox="0 0 7 9"
      shapeRendering="crispEdges"
      aria-hidden="true"
    >
      {cells(ROBOT).map(({ cell, x, y }) => (
        <rect key={`${x},${y}`} className={cell === 'a' ? 'robot-antenna' : undefined} x={x} y={y} width={1} height={1} fill={ROBOT_COLORS[cell]} />
      ))}
    </svg>
  )
}

function useSeconds(): number {
  const [seconds, setSeconds] = useState(1)
  useEffect(() => {
    if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return
    const started = performance.now()
    const timer = setInterval(() => setSeconds((performance.now() - started) / 1000), 100)
    return () => clearInterval(timer)
  }, [])
  return seconds
}

export function Mascot({ scale = 2 }: { scale?: number }) {
  const time = useSeconds()
  const eyeX = 3 + (Math.floor(time / 2.2) % 4 === 1 ? 1 : 0)
  const closed = time % 3.2 < 0.15
  return (
    <svg className="inline-block shrink-0 align-middle" width={9 * scale} height={11 * scale} viewBox="0 0 9 11" shapeRendering="crispEdges" aria-hidden="true">
      <rect x={2} y={2} width={5} height={5} fill="#ffffff" />
      {cells(RING).map(({ cell, x, y }) => (
        <rect key={`${x},${y}`} x={x} y={y} width={1} height={1} fill={RING_COLORS[cell]} />
      ))}
      {[eyeX, eyeX + 2].map((x) => (
        <rect key={x} x={x} y={closed ? 4 : 3} width={1} height={closed ? 1 : 3} fill={CORE} />
      ))}
      <rect x={2} y={9} width={2} height={2} fill={CORE} />
      <rect x={5} y={9} width={2} height={2} fill={CORE} />
    </svg>
  )
}

export function Logo({ size = 22 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 100 100" aria-hidden="true">
      <path d="M74.15 29.01 A32 32 0 0 1 74.15 70.99" fill="none" stroke="#a8acf5" strokeWidth="18" />
      <path d="M70.99 74.15 A32 32 0 0 1 29.01 74.15" fill="none" stroke="#7e85f2" strokeWidth="18" />
      <path d="M25.85 70.99 A32 32 0 0 1 25.85 29.01" fill="none" stroke="#5d64e6" strokeWidth="18" />
      <path d="M29.01 25.85 A32 32 0 0 1 70.99 25.85" fill="none" stroke="#3d44c9" strokeWidth="18" />
      <circle cx="50" cy="50" r="14" fill={CORE} />
    </svg>
  )
}
