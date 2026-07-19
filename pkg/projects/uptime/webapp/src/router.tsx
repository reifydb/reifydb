// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import {
  Outlet,
  createRootRoute,
  createRoute,
  createRouter,
  redirect,
} from '@tanstack/react-router'
import { AppLayout } from '@/components/layout/app-layout.tsx'
import { AuthLayout } from '@/components/layout/auth-layout.tsx'
import { RequireAuth } from '@/components/auth/require-auth.tsx'
import { LoginPage } from '@/pages/login'
import { RegisterPage } from '@/pages/register'
import { DashboardPage } from '@/pages/dashboard'
import { MonitorNewPage } from '@/pages/monitors/new.tsx'
import { MonitorDetailPage } from '@/pages/monitors/detail.tsx'
import { MonitorEditPage } from '@/pages/monitors/edit.tsx'
import { StatusPagesPage } from '@/pages/status-pages'
import { StatusPageNewPage, StatusPageEditPage } from '@/pages/status-pages/form.tsx'
import { PublicStatusPage } from '@/pages/public-status'

const rootRoute = createRootRoute({
  component: () => <Outlet />,
})

const publicStatusRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/status/$slug',
  component: PublicStatusPage,
})

const authLayoutRoute = createRoute({
  getParentRoute: () => rootRoute,
  id: 'auth-layout',
  component: AuthLayout,
})

const loginRoute = createRoute({
  getParentRoute: () => authLayoutRoute,
  path: '/login',
  component: LoginPage,
})

const registerRoute = createRoute({
  getParentRoute: () => authLayoutRoute,
  path: '/register',
  component: RegisterPage,
})

function AppShell() {
  return (
    <RequireAuth>
      <AppLayout />
    </RequireAuth>
  )
}

const appRoute = createRoute({
  getParentRoute: () => rootRoute,
  id: 'app',
  component: AppShell,
})

const indexRoute = createRoute({
  getParentRoute: () => appRoute,
  path: '/',
  beforeLoad: () => {
    throw redirect({ to: '/monitors' })
  },
})

const dashboardRoute = createRoute({
  getParentRoute: () => appRoute,
  path: '/monitors',
  component: DashboardPage,
})

const monitorNewRoute = createRoute({
  getParentRoute: () => appRoute,
  path: '/monitors/new',
  component: MonitorNewPage,
})

const monitorDetailRoute = createRoute({
  getParentRoute: () => appRoute,
  path: '/monitors/$monitorId',
  component: MonitorDetailPage,
})

const monitorEditRoute = createRoute({
  getParentRoute: () => appRoute,
  path: '/monitors/$monitorId/edit',
  component: MonitorEditPage,
})

const statusPagesRoute = createRoute({
  getParentRoute: () => appRoute,
  path: '/status-pages',
  component: StatusPagesPage,
})

const statusPageNewRoute = createRoute({
  getParentRoute: () => appRoute,
  path: '/status-pages/new',
  component: StatusPageNewPage,
})

const statusPageEditRoute = createRoute({
  getParentRoute: () => appRoute,
  path: '/status-pages/$pageId/edit',
  component: StatusPageEditPage,
})

const routeTree = rootRoute.addChildren([
  publicStatusRoute,
  authLayoutRoute.addChildren([loginRoute, registerRoute]),
  appRoute.addChildren([
    indexRoute,
    dashboardRoute,
    monitorNewRoute,
    monitorDetailRoute,
    monitorEditRoute,
    statusPagesRoute,
    statusPageNewRoute,
    statusPageEditRoute,
  ]),
])

export const router = createRouter({ routeTree })
