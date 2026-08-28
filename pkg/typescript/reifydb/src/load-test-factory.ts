// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { ReifydbNode } from '../native'
import type { TestFactory } from './db'
import { NativeDb } from './native-db'

const cache = new Map<string, TestFactory>()

export function loadTestFactory(addonPath: string): TestFactory {
  const cached = cache.get(addonPath)
  if (cached != null) return cached

  const addon = { exports: {} as { create: (seed: number) => ReifydbNode } }
  try {
    process.dlopen(addon as unknown as NodeJS.Module, addonPath)
  } catch (err) {
    throw new Error(`failed to load native addon at ${addonPath}: ${err}`)
  }

  const factory: TestFactory = (seed) => new NativeDb(addon.exports.create(seed))
  cache.set(addonPath, factory)
  return factory
}
