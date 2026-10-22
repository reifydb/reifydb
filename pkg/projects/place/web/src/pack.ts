// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { ListValue, Uint8Value } from '@reifydb/core'

export interface Pixel {
  x: number
  y: number
  color: number
}

export function pack({ x, y, color }: Pixel): Uint8Value {
  return new Uint8Value(BigInt(x) * 2097152n + BigInt(y) * 256n + BigInt(color))
}

export function pixelsParam(pixels: readonly Pixel[]): ListValue {
  return new ListValue(pixels.map(pack), 'Uint8')
}
