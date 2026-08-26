// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { vi } from 'vitest'

export const navigate = vi.fn()

// Substitutes $params so href assertions exercise the same route template the real Link would resolve.
export function routerMock() {
  return {
    useNavigate: () => navigate,
    Link: ({ to, params, children, ...rest }: any) => {
      const href = params
        ? Object.entries(params).reduce((acc: string, [k, v]) => acc.replace(`$${k}`, String(v)), to)
        : to
      return (
        <a href={href} {...rest}>
          {children}
        </a>
      )
    },
  }
}
