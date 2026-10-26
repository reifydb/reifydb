// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub fn row_of(len: usize, row: usize) -> usize {
	if len == 1 {
		0
	} else {
		row
	}
}

pub fn is_constant(len: usize, row_count: usize) -> bool {
	len == 1 && row_count != 1
}
