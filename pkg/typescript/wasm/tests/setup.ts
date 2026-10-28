// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { beforeAll } from 'vitest';
import { createWasmDb } from '../src/index';

beforeAll(async () => {
  // Without this, the first test of each file pays the module compile and can blow its 30s limit.
  (await createWasmDb()).free();
}, 120_000);
