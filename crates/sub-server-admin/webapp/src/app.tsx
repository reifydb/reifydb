import { RouterProvider } from '@tanstack/react-router'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { getConnection } from '@reifydb/react'
import { router } from './router'
import { REIFYDB_CONFIG } from './config'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 1000 * 60 * 5, // 5 minutes
      gcTime: 1000 * 60 * 10, // 10 minutes
    },
  },
})

function App() {
  // Initialize connection configuration
  getConnection({
    url: REIFYDB_CONFIG.getWebSocketUrl(),
    options: {
      timeoutMs: REIFYDB_CONFIG.CONNECTION.TIMEOUT_MS,
    }
  })

  return (
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  )
}

export default App