import { RouterProvider } from '@tanstack/react-router'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ConnectionProvider } from '@reifydb/react'
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
  return (
    <ConnectionProvider config={{
      url: REIFYDB_CONFIG.getWebSocketUrl(),
      options: {
        timeoutMs: REIFYDB_CONFIG.CONNECTION.TIMEOUT_MS,
      }
    }}>
      <QueryClientProvider client={queryClient}>
        <RouterProvider router={router} />
      </QueryClientProvider>
    </ConnectionProvider>
  )
}

export default App