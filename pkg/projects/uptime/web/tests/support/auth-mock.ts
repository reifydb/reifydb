// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { vi } from 'vitest'

export function authMock() {
  return {
    useAuth: () => ({ session: { token: 'test-token' }, signOut: vi.fn() }),
  }
}
