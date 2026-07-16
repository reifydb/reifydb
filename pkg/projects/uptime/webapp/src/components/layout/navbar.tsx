// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Link, useLocation } from '@tanstack/react-router'
import { LogOut } from 'lucide-react'
import { useAuth } from '@reifydb/auth'
import { Button } from '@reifydb/ui'

const navigation = [
  { name: 'Dashboard', href: '/dashboard' },
  { name: 'Status Pages', href: '/status-pages' },
]

function isActive(pathname: string, href: string): boolean {
  if (pathname === href || pathname.startsWith(`${href}/`)) return true
  return (
    href === '/dashboard' && (pathname === '/' || pathname.startsWith('/monitors'))
  )
}

export function Navbar() {
  const location = useLocation()
  const { session, signOut } = useAuth()

  const email = session?.identifier ?? session?.wallet_address ?? ''

  return (
    <header className="sticky top-0 z-40 w-full border-b-2 border-border-default bg-bg-primary">
      <div className="mx-auto flex h-[60px] max-w-6xl items-center justify-between px-4 sm:px-6">
        <Link to="/dashboard" className="font-mono text-lg font-bold tracking-tight text-text-primary">
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
