// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import type { TestFactory } from './db'

const cache = new Map<string, TestFactory>()

export function load_test_factory(addonPath: string): TestFactory {
  const cached = cache.get(addonPath)
  if (cached != null) return cached

  const addon = { exports: {} as { create: TestFactory } }
  try {
    process.dlopen(addon as unknown as NodeJS.Module, addonPath)
  } catch (err) {
    throw new Error(`failed to load native addon at ${addonPath}: ${err}`)
  }

  cache.set(addonPath, addon.exports.create)
  return addon.exports.create
}
