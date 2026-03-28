// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { ReactNode } from 'react'

interface EmptyStateProps {
  icon?: string
  title: string
  description?: string
  children?: ReactNode
}

export function EmptyState({ icon = '~', title, description, children }: EmptyStateProps) {
  return (
    <div className="border-2 border-dashed border-black/15 py-16 px-8 text-center">
      <div className="text-4xl font-mono text-text-muted mb-4">[{icon}]</div>
      <h3 className="text-lg font-bold text-text-primary mb-2">{title}</h3>
      {description && (
        <p className="text-sm text-text-secondary max-w-md mx-auto mb-6">{description}</p>
      )}
      {children}
    </div>
  )
}
