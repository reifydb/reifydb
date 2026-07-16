// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { type ReactNode } from 'react'

export function PublicLayout({ children }: { children: ReactNode }) {
  return (
    <div className="min-h-screen bg-background">
      <header className="border-b border-border bg-card">
        <div className="max-w-3xl mx-auto flex items-center gap-3 px-4 py-4">
          <img
            src="/assets/favicon-128x128.png"
            alt="ReifyDB Logo"
            className="h-8 w-8 object-contain"
          />
          <span className="font-semibold">ReifyDB Uptime</span>
        </div>
      </header>
      <main className="max-w-3xl mx-auto px-4 py-8">{children}</main>
      <footer className="max-w-3xl mx-auto px-4 py-6 text-xs text-muted-foreground">
        Powered by ReifyDB
      </footer>
    </div>
  )
}
