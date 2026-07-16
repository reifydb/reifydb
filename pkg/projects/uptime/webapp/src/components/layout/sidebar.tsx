// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Link, useLocation, useNavigate } from '@tanstack/react-router'
import { Activity, LayoutDashboard, LogOut, PanelsTopLeft } from 'lucide-react'
import { useAuth } from '@reifydb/auth'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'

const navigation = [
  { name: 'Dashboard', href: '/dashboard', icon: LayoutDashboard },
  { name: 'Status Pages', href: '/status-pages', icon: PanelsTopLeft },
]

export function Sidebar() {
  const location = useLocation()
  const navigate = useNavigate()
  const { session, signOut } = useAuth()

  const email = session?.identifier ?? session?.wallet_address ?? ''

  return (
    <div className="relative flex flex-col h-full w-64 bg-secondary text-secondary-foreground border-r border-border">
      <div className="flex items-center gap-2 p-4 border-b">
        <div
          className="flex items-center gap-2 cursor-pointer hover:opacity-80 transition-opacity"
          onClick={() => navigate({ to: '/dashboard' })}
        >
          <Activity className="h-6 w-6" />
          <span className="font-semibold text-lg">Uptime</span>
        </div>
      </div>

      <nav className="flex-1 p-2 space-y-1">
        {navigation.map((item) => {
          const isActive =
            location.pathname === item.href ||
            location.pathname.startsWith(`${item.href}/`) ||
            (item.href === '/dashboard' &&
              (location.pathname === '/' || location.pathname.startsWith('/monitors')))
          return (
            <Link
              key={item.name}
              to={item.href}
              className={cn(
                'flex items-center gap-3 px-3 py-2 text-sm font-medium transition-all duration-200',
                isActive
                  ? 'bg-primary text-primary-foreground'
                  : 'text-secondary-foreground/70 hover:text-secondary-foreground hover:bg-secondary-foreground/10',
              )}
            >
              <item.icon className="h-4 w-4 flex-shrink-0" />
              <span>{item.name}</span>
            </Link>
          )
        })}
      </nav>

      <div className="p-4 border-t space-y-3">
        <p className="text-xs text-muted-foreground truncate" title={email}>
          {email}
        </p>
        <Button
          variant="outline"
          size="sm"
          className="w-full justify-start"
          onClick={() => void signOut()}
        >
          <LogOut className="h-4 w-4" />
          Sign out
        </Button>
      </div>
    </div>
  )
}
