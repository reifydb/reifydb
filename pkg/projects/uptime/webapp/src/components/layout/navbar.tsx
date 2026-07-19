// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect, useRef, useState } from 'react'
import { Link, useLocation } from '@tanstack/react-router'
import { ChevronDown, LogOut } from 'lucide-react'
import { useAuth } from '@reifydb/auth'

const navigation = [
  { name: 'Monitors', href: '/monitors' },
  { name: 'Status Pages', href: '/status-pages' },
]

function isActive(pathname: string, href: string): boolean {
  if (pathname === href || pathname.startsWith(`${href}/`)) return true
  return href === '/monitors' && pathname === '/'
}

function UserMenu({ email, onSignOut }: { email: string; onSignOut: () => void }) {
  const [open, setOpen] = useState(false)
  const containerRef = useRef<HTMLDivElement>(null)
  const initial = email.trim().charAt(0).toUpperCase() || '?'

  useEffect(() => {
    if (!open) return
    function onPointerDown(event: MouseEvent) {
      if (
        containerRef.current != null &&
        !containerRef.current.contains(event.target as Node)
      ) {
        setOpen(false)
      }
    }
    function onKeyDown(event: KeyboardEvent) {
      if (event.key === 'Escape') setOpen(false)
    }
    document.addEventListener('mousedown', onPointerDown)
    document.addEventListener('keydown', onKeyDown)
    return () => {
      document.removeEventListener('mousedown', onPointerDown)
      document.removeEventListener('keydown', onKeyDown)
    }
  }, [open])

  return (
    <div ref={containerRef} className="relative">
      <button
        type="button"
        onClick={() => setOpen((prev) => !prev)}
        aria-haspopup="menu"
        aria-expanded={open}
        className="flex items-center gap-1 rounded-full outline-none focus-visible:ring-2 focus-visible:ring-primary"
      >
        <span
          title={email}
          className="flex h-8 w-8 items-center justify-center rounded-full border-2 border-border-default bg-bg-tertiary font-mono text-xs font-bold uppercase text-text-primary"
        >
          {initial}
        </span>
        <ChevronDown
          className={`h-4 w-4 text-text-muted transition-transform duration-150 ${
            open ? 'rotate-180' : ''
          }`}
        />
      </button>
      {open && (
        <div
          role="menu"
          className="glass-card absolute right-0 top-full z-50 mt-2 w-56 overflow-hidden p-1"
        >
          <p
            className="truncate px-3 py-2 font-mono text-xs text-text-muted"
            title={email}
          >
            {email || 'Signed in'}
          </p>
          <div className="border-t border-border-light" />
          <button
            type="button"
            role="menuitem"
            onClick={() => {
              setOpen(false)
              onSignOut()
            }}
            className="flex w-full items-center gap-2 rounded px-3 py-2 text-left text-xs text-text-secondary transition-colors hover:bg-bg-tertiary hover:text-primary-dark"
          >
            <LogOut className="h-4 w-4" />
            Sign out
          </button>
        </div>
      )}
    </div>
  )
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
        <UserMenu email={email} onSignOut={() => void signOut()} />
      </div>
    </header>
  )
}
