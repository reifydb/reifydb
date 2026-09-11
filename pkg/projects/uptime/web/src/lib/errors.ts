// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { ReifyError } from '@reifydb/react'

const recordedErrors = new WeakSet<object>()

export function errorMessage(err: Error): string {
  if (!(err instanceof ReifyError)) return err.message
  const prefix = `[${err.code}] `
  const suffix = err.label ? ` \u2014 ${err.label}` : ''
  return err.message.slice(prefix.length, err.message.length - suffix.length)
}

export async function recorded<T>(pending: Promise<T>): Promise<T> {
  try {
    return await pending
  } catch (err) {
    if (typeof err === 'object' && err !== null) recordedErrors.add(err)
    throw err
  }
}

export function rethrowUnrecorded(err: unknown): void {
  if (typeof err !== 'object' || err === null || !recordedErrors.has(err)) throw err
}
