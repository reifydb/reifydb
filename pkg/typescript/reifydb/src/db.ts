// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

export interface Db {
  command_root(rql: string): Promise<string>
  query_root(rql: string): Promise<string>
  command_as(identity: string, rql: string): Promise<string>
  query_as(identity: string, rql: string): Promise<string>
  authenticate(method: string, credentials: Record<string, string>): Promise<string>
}

export interface TestDb extends Db {}

export interface TestFactory {
  (seed: number): TestDb
}
