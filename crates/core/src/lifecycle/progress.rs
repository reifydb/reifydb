// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
	Yielded,
	Exhausted,
}

impl Progress {
	pub fn is_yielded(self) -> bool {
		matches!(self, Progress::Yielded)
	}

	pub fn is_exhausted(self) -> bool {
		matches!(self, Progress::Exhausted)
	}
}
