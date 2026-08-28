// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { type ReactNode } from 'react'

export function PublicLayout({ children }: { children: ReactNode }) {
  return (
    <div className="min-h-screen bg-bg-primary">
      <header className="border-b-2 border-border-default bg-bg-primary">
        <div className="mx-auto flex h-[60px] max-w-3xl items-center px-4">
          <span className="font-mono font-bold text-text-primary">Uptime</span>
        </div>
      </header>
      <main className="max-w-3xl mx-auto px-4 py-8">{children}</main>
      <footer className="max-w-3xl mx-auto px-4 py-6 font-mono text-xs text-text-muted">
        Powered by ReifyDB
      </footer>
    </div>
  )
}
