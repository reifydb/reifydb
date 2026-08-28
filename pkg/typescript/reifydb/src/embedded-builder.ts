// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { openWithMigrations } from '../native'
import type { Db } from './db'
import type { MigrationInput } from './migration'
import { NativeDb } from './native-db'

export class EmbeddedBuilder {
  private readonly migrations: MigrationInput[]

  constructor(migrations: MigrationInput[] = []) {
    this.migrations = migrations
  }

  withMigrations(input: MigrationInput | MigrationInput[]): EmbeddedBuilder {
    return new EmbeddedBuilder(Array.isArray(input) ? input : [input])
  }

  build(): Db {
    return new NativeDb(openWithMigrations(this.migrations.map(toEntry)))
  }
}

function toEntry(input: MigrationInput) {
  if ('dir' in input) return { dir: input.dir }
  return { name: input.name, statements: input.statements, rollback: input.rollback }
}
