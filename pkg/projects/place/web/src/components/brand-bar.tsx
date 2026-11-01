// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { ReactNode } from 'react'
export function BrandBar({ tag, children }: { tag?: string; children?: ReactNode }) {
  return (
    <header className="z-40 w-full shrink-0 border-b border-border-light bg-bg-primary">
      <div className="flex h-14 w-full items-center gap-3 px-4">
        <a href="/" className="flex items-center gap-2 text-text-primary no-underline">
          <span className="font-serif text-2xl leading-none">Place</span>
        </a>
        {tag && <span className="label hidden text-text-muted sm:inline">{tag}</span>}
        <div className="ml-auto flex items-center gap-4 text-sm">
          {children}
          <span className="hidden items-center gap-1.5 text-text-muted md:flex">
            Powered by
            <a href="https://reifydb.com" target="_blank" rel="noopener noreferrer" className="text-text-primary underline underline-offset-4">
              ReifyDB
            </a>
          </span>
        </div>
      </div>
    </header>
  )
}
