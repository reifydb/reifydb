// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::count::Count;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimOutcome {
	pub removed: Count,
	pub more: bool,
}

impl ReclaimOutcome {
	pub const NOTHING: Self = Self {
		removed: Count::ZERO,
		more: false,
	};
}
