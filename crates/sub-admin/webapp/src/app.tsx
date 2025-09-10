import { RouterProvider } from '@tanstack/react-router'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ReactQueryDevtools } from '@tanstack/react-query-devtools'
import { connection } from '@reifydb/react'
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
  connection.setConfig({
    url: REIFYDB_CONFIG.getWebSocketUrl(),
    options: {
      timeoutMs: REIFYDB_CONFIG.CONNECTION.TIMEOUT_MS,
    }
  })

  return (
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
      <ReactQueryDevtools initialIsOpen={false} />
    </QueryClientProvider>
  )
}

export default App