// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Link, useLocation } from '@tanstack/react-router'
import { LogOut } from 'lucide-react'
import { useAuth } from '@reifydb/auth'
import { Button } from '@reifydb/ui'
import { useConnectionStatus, type ConnectionStatus } from '@/store/realtime'

const CONNECTION_STYLE: Record<ConnectionStatus, { dot: string; label: string }> = {
  live: { dot: 'bg-status-success', label: 'live' },
  connecting: { dot: 'bg-status-warning animate-pulse', label: 'connecting' },
  reconnecting: { dot: 'bg-status-warning animate-pulse', label: 'reconnecting' },
  offline: { dot: 'bg-text-muted', label: 'offline' },
}

function ConnectionIndicator() {
  const status = useConnectionStatus()
  const style = CONNECTION_STYLE[status]
  return (
    <span className="hidden items-center gap-1.5 font-mono text-[10px] uppercase tracking-[1.4px] text-text-muted sm:inline-flex">
      <span className={`inline-block h-2 w-2 rounded-full ${style.dot}`} />
      {style.label}
    </span>
  )
}

const navigation = [
  { name: 'Monitors', href: '/monitors' },
  { name: 'Status Pages', href: '/status-pages' },
]

function isActive(pathname: string, href: string): boolean {
  if (pathname === href || pathname.startsWith(`${href}/`)) return true
  return href === '/monitors' && pathname === '/'
}

export function Navbar() {
  const location = useLocation()
  const { session, signOut } = useAuth()

  const email = session?.identifier ?? session?.wallet_address ?? ''

  return (
    <header className="sticky top-0 z-40 w-full border-b-2 border-border-default bg-bg-primary">
      <div className="mx-auto flex h-[60px] max-w-6xl items-center justify-between px-4 sm:px-6">
        <Link to="/monitors" className="font-mono text-lg font-bold tracking-tight text-text-primary">
          Uptime
        </Link>
        <nav className="flex items-center font-mono text-xs">
          {navigation.map((item) => (
            <Link
              key={item.href}
              to={item.href}
              className={`px-3 py-2 uppercase tracking-[1.4px] transition-colors duration-150 ${
                isActive(location.pathname, item.href)
                  ? 'font-bold text-primary-dark'
                  : 'text-text-secondary hover:text-primary-dark'
              }`}
            >
              {item.name}
            </Link>
          ))}
        </nav>
        <div className="flex items-center gap-3">
          <ConnectionIndicator />
          <span
            className="hidden max-w-48 truncate font-mono text-xs text-text-muted sm:inline"
            title={email}
          >
            {email}
          </span>
          <Button variant="ghost" size="sm" onClick={() => void signOut()}>
            <LogOut className="h-4 w-4" />
            Sign out
          </Button>
        </div>
      </div>
    </header>
  )
}
