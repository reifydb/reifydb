// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { RouterProvider } from '@tanstack/react-router'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { AuthProvider, json_http_transport } from '@reifydb/auth'
import { router } from './router'
import { UPTIME_CONFIG } from './config'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 1000 * 30,
      gcTime: 1000 * 60 * 10,
    },
  },
})

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <AuthProvider
        url={UPTIME_CONFIG.authUrl()}
        transport={json_http_transport}
        storageNamespace={UPTIME_CONFIG.storageNamespace}
        sessionScope="browser"
        sessionTtlSeconds={UPTIME_CONFIG.sessionTtlSeconds}
      >
        <RouterProvider router={router} />
      </AuthProvider>
    </QueryClientProvider>
  )
}

export default App
