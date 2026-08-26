// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { open_with_migrations } from '../native'
import type { Db } from './db'
import type { MigrationInput } from './migration'

export class EmbeddedBuilder {
  private readonly migrations: MigrationInput[]

  constructor(migrations: MigrationInput[] = []) {
    this.migrations = migrations
  }

  with_migrations(input: MigrationInput | MigrationInput[]): EmbeddedBuilder {
    return new EmbeddedBuilder(Array.isArray(input) ? input : [input])
  }

  build(): Db {
    return open_with_migrations(this.migrations.map(to_entry))
  }
}

function to_entry(input: MigrationInput) {
  if ('dir' in input) return { dir: input.dir }
  return { name: input.name, statements: input.statements, rollback: input.rollback }
}
