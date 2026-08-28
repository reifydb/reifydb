// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

export interface Migration {
  name: string
  statements: string[]
  rollback?: string[]
}

export type MigrationInput = Migration | { dir: string }
