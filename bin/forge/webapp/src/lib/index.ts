// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

export function cn(...classes: (string | undefined | null | false)[]): string {
  return classes.filter(Boolean).join(' ')
}
