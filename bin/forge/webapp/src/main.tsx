import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import { ConnectionProvider } from '@reifydb/react'
import { App } from './app'
import { FORGE_CONFIG } from './config'
import './index.css'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <ConnectionProvider config={{
      url: FORGE_CONFIG.getWebSocketUrl(),
      options: { timeoutMs: FORGE_CONFIG.CONNECTION.TIMEOUT_MS },
    }}>
      <BrowserRouter>
        <App />
      </BrowserRouter>
    </ConnectionProvider>
  </StrictMode>,
)
