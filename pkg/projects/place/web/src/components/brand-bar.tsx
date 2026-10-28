// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { ReactNode } from 'react'
import { Logo, Mascot } from './sprites'

export function BrandBar({ tag, children }: { tag?: string; children?: ReactNode }) {
  return (
    <header className="z-40 w-full shrink-0 border-b-2 border-[#4b52d6] bg-white">
      <div className="flex h-14 w-full items-center gap-3 px-4">
        <a href="/" className="flex items-center gap-2 text-text-primary no-underline">
          <Mascot scale={2} />
          <span className="text-lg font-extrabold tracking-tight">place</span>
        </a>
        {tag && <span className="pixel-label hidden text-text-muted sm:inline">{tag}</span>}
        <span className="pixel-label flex items-center gap-1.5 text-primary-dark">
          <span className="live-led" />
          LIVE
        </span>
        <div className="ml-auto flex items-center gap-4 text-sm">
          {children}
          <span className="hidden items-center gap-1.5 text-text-muted md:flex">
            Powered by <Logo size={16} />
            <a href="https://reifydb.com" target="_blank" rel="noopener noreferrer" className="font-bold text-primary-dark underline underline-offset-4">
              ReifyDB
            </a>
          </span>
        </div>
      </div>
    </header>
  )
}
