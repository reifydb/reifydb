// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { EmbeddedBuilder } from './embedded-builder'

export class Reifydb {
  static memory(): EmbeddedBuilder {
    return new EmbeddedBuilder()
  }
}
