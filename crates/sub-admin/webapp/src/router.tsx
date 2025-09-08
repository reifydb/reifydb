import { createRootRoute, createRoute, createRouter, redirect } from '@tanstack/react-router'
import { MainLayout } from '@/components/layout/main-layout.tsx'
import { DashboardPage } from '@/pages/dashboard'
import { SchemaPage } from '@/pages/schema'
import { QueryPage } from '@/pages/query'
import { SettingsPage } from '@/pages/settings'
import { UsersPage } from '@/pages/users'

const rootRoute = createRootRoute({
  component: MainLayout,
})

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/',
  beforeLoad: () => {
    throw redirect({
      to: '/dashboard'
    })
  }
})

const dashboardRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/dashboard',
  component: DashboardPage,
})

const schemaRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/schema',
  component: SchemaPage,
})

const queryRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/query',
  component: QueryPage,
})

const settingsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/settings',
  component: SettingsPage,
})

const usersRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/users',
  component: UsersPage,
})

const routeTree = rootRoute.addChildren([
  indexRoute,
  dashboardRoute,
  schemaRoute,
  queryRoute,
  settingsRoute,
  usersRoute,
])

export const router = createRouter({ routeTree })