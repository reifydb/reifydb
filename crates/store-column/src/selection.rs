// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::BooleanBuffer;

#[derive(Clone, Debug)]
pub enum Selection {
	All,
	None_,
	Mask(BooleanBuffer),
}
