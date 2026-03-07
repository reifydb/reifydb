// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use subtle::ConstantTimeEq;

pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
	a.ct_eq(b).into()
}
