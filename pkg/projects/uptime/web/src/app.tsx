// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { RouterProvider } from '@tanstack/react-router'
import { AuthProvider, jsonHttpTransport } from '@reifydb/auth'
import { router } from './router'
import { UPTIME_CONFIG } from './config'

function App() {
  return (
    <AuthProvider
      url={UPTIME_CONFIG.authUrl()}
      transport={jsonHttpTransport}
      storageNamespace={UPTIME_CONFIG.storageNamespace}
      sessionScope="browser"
      sessionTtlSeconds={UPTIME_CONFIG.sessionTtlSeconds}
    >
      <RouterProvider router={router} />
    </AuthProvider>
  )
}

export default App
