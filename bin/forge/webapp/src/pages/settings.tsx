// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useConnection } from '@reifydb/react'
import { FORGE_CONFIG } from '@/config'
import { cn } from '@/lib'

export function SettingsPage() {
  const { isConnected, isConnecting, connectionError, reconnect } = useConnection()

  return (
    <div className="mx-auto max-w-6xl px-6 md:px-8 py-8">
      {/* Header */}
      <div className="mb-8">
        <div className="flex items-center gap-2 text-xs font-mono text-text-muted mb-2">
          <span className="text-primary">$</span> forge config
        </div>
        <h1 className="text-2xl sm:text-3xl font-black tracking-tight">Settings</h1>
      </div>

      {/* Connection */}
      <div className="border border-dashed border-black/25 mb-6">
        <div className="px-4 py-2.5 border-b border-dashed border-black/25 bg-bg-secondary">
          <span className="text-xs font-bold uppercase tracking-wider text-text-muted"># Connection</span>
        </div>
        <div className="p-4 space-y-4">
          <div className="flex items-center justify-between">
            <span className="text-sm font-mono text-text-secondary">WebSocket URL</span>
            <span className="text-sm font-mono text-text-primary">{FORGE_CONFIG.getWebSocketUrl()}</span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-sm font-mono text-text-secondary">Status</span>
            <div className="flex items-center gap-2">
              <span className={cn(
                'inline-block w-2 h-2',
                isConnected ? 'bg-status-success' : isConnecting ? 'bg-status-warning' : 'bg-status-error',
              )} />
              <span className="text-sm font-mono">
                {isConnected ? 'connected' : isConnecting ? 'connecting...' : 'disconnected'}
              </span>
            </div>
          </div>
          {connectionError && (
            <div className="text-sm font-mono text-status-error bg-status-error/10 border border-status-error/20 p-3">
              {connectionError}
            </div>
          )}
          <div className="pt-2">
            <button
              onClick={() => reconnect()}
              className="text-xs font-mono text-primary hover:underline"
            >
              [reconnect]
            </button>
          </div>
        </div>
      </div>

      {/* About */}
      <div className="border border-dashed border-black/25">
        <div className="px-4 py-2.5 border-b border-dashed border-black/25 bg-bg-secondary">
          <span className="text-xs font-bold uppercase tracking-wider text-text-muted"># About</span>
        </div>
        <div className="p-4 space-y-3">
          <div className="flex items-center justify-between">
            <span className="text-sm font-mono text-text-secondary">Platform</span>
            <span className="text-sm font-mono text-text-primary">Forge CI</span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-sm font-mono text-text-secondary">Powered by</span>
            <span className="text-sm font-mono text-primary">ReifyDB</span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-sm font-mono text-text-secondary">Timeout</span>
            <span className="text-sm font-mono text-text-primary">{FORGE_CONFIG.CONNECTION.TIMEOUT_MS}ms</span>
          </div>
        </div>
      </div>
    </div>
  )
}
