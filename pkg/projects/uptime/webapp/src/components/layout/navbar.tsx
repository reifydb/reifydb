// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect, useRef, useState, type ReactNode } from 'react'
import { Link, useLocation } from '@tanstack/react-router'
import { ChevronDown, LogIn, LogOut, UserPlus } from 'lucide-react'
import { useMe } from '@/hooks/use-me'
import { useSignOut } from '@/hooks/use-sign-out'

const navigation = [
  { name: 'Monitors', href: '/monitors' },
  { name: 'Probes', href: '/probes' },
  { name: 'Status Pages', href: '/status-pages' },
]

function isActive(pathname: string, href: string): boolean {
  if (pathname === href || pathname.startsWith(`${href}/`)) return true
  return href === '/monitors' && pathname === '/'
}

const menuItemClass =
  'flex w-full items-center gap-2 rounded px-3 py-2 text-left text-xs text-text-secondary transition-colors hover:bg-bg-tertiary hover:text-primary-dark'

function GuestMenuItems({ onNavigate }: { onNavigate: () => void }) {
  return (
    <>
      <Link to="/register" className={menuItemClass} role="menuitem" onClick={onNavigate}>
        <UserPlus className="h-4 w-4" />
        Create account
      </Link>
      <Link to="/login" className={menuItemClass} role="menuitem" onClick={onNavigate}>
        <LogIn className="h-4 w-4" />
        Sign in
      </Link>
    </>
  )
}

function UserMenu({
  label,
  initial,
  children,
}: {
  label: string
  initial: string
  children: (close: () => void) => ReactNode
}) {
  const [open, setOpen] = useState(false)
  const containerRef = useRef<HTMLDivElement>(null)

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
          title={label}
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
            title={label}
          >
            {label}
          </p>
          <div className="border-t border-border-light" />
          {children(() => setOpen(false))}
        </div>
      )}
    </div>
  )
}

export function Navbar() {
  const location = useLocation()
  const { data: me } = useMe()
  const signOut = useSignOut()

  const guest = me?.guest === true
  const email = me?.email ?? ''
  const label = guest ? 'Guest session' : email || 'Signed in'
  const initial = guest ? 'G' : email.trim().charAt(0).toUpperCase() || '?'

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
        <UserMenu label={label} initial={initial}>
          {(close) =>
            guest ? (
              <GuestMenuItems onNavigate={close} />
            ) : (
              <button
                type="button"
                role="menuitem"
                onClick={() => {
                  close()
                  void signOut()
                }}
                className={menuItemClass}
              >
                <LogOut className="h-4 w-4" />
                Sign out
              </button>
            )
          }
        </UserMenu>
      </div>
    </header>
  )
}
