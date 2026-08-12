// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimOutcome {
	pub removed: usize,
	pub more: bool,
}

impl ReclaimOutcome {
	pub const NOTHING: Self = Self {
		removed: 0,
		more: false,
	};
}

