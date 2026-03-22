// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useState } from 'react'
import { Link, useLocation } from 'react-router-dom'
import { cn } from '@/lib'
import { useConnection } from '@reifydb/react'

const navLinks = [
  { label: 'Dashboard', href: '/' },
  { label: 'Pipelines', href: '/pipelines' },
  { label: 'Settings', href: '/settings' },
]

export function Navbar() {
  const [mobileOpen, setMobileOpen] = useState(false)
  const location = useLocation()
  const { isConnected, isConnecting } = useConnection()

  const isActive = (href: string) => {
    if (href === '/') return location.pathname === '/'
    return location.pathname.startsWith(href)
  }

  return (
    <header className="sticky top-0 z-40 w-full bg-[rgba(255,255,255,0.95)] border-b border-dashed border-black/25">
      <div className="flex h-[60px] w-full items-center justify-between px-4 sm:px-6 md:pl-8 md:pr-16">
        {/* Logo */}
        <Link to="/" className="flex items-center gap-2">
          <span className="text-primary font-mono">$</span>
          <span className="font-bold text-lg tracking-tight text-text-primary">
            Forge
          </span>
          <span className="text-text-muted text-xs font-mono">CI</span>
        </Link>

        {/* Desktop Navigation */}
        <nav className="hidden md:flex gap-0 text-sm font-mono items-center flex-1 justify-center">
          {navLinks.map((link) => {
            const active = isActive(link.href)
            return (
              <Link
                key={link.href}
                to={link.href}
                className={cn(
                  'px-3 py-2 transition-colors duration-150',
                  active
                    ? 'text-primary'
                    : 'text-text-secondary hover:text-primary'
                )}
              >
                [{active && '*'}{link.label}]
              </Link>
            )
          })}
        </nav>

        {/* Status + Mobile */}
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2 text-xs font-mono">
            <span className={cn(
              'inline-block w-2 h-2',
              isConnected ? 'bg-status-success' : isConnecting ? 'bg-status-warning' : 'bg-status-error',
            )} />
            <span className="text-text-muted hidden sm:inline">
              {isConnected ? 'connected' : isConnecting ? 'connecting...' : 'disconnected'}
            </span>
          </div>

          {/* Hamburger */}
          <button
            onClick={() => setMobileOpen(!mobileOpen)}
            className="md:hidden font-mono text-sm text-text-muted hover:text-primary transition-colors"
            aria-label="Toggle menu"
          >
            {mobileOpen ? '[x]' : '[=]'}
          </button>
        </div>
      </div>

      {/* Mobile Menu */}
      {mobileOpen && (
        <div className="md:hidden border-t border-dashed border-black/25 bg-bg-primary">
          <nav className="flex flex-col p-4 gap-1 text-sm font-mono">
            {navLinks.map((link) => {
              const active = isActive(link.href)
              return (
                <Link
                  key={link.href}
                  to={link.href}
                  onClick={() => setMobileOpen(false)}
                  className={cn(
                    'px-3 py-2 transition-colors duration-150',
                    active ? 'text-primary' : 'text-text-secondary hover:text-primary'
                  )}
                >
                  [{active && '*'}{link.label}]
                </Link>
              )
            })}
          </nav>
        </div>
      )}
    </header>
  )
}
