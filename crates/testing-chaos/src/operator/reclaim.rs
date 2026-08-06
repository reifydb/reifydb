// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StateFootprint {
	pub data_rows: usize,

	pub node_scoped_data_rows: usize,

	pub identity_rows: usize,
}

impl StateFootprint {
	pub fn total(&self) -> usize {
		self.data_rows + self.identity_rows
	}
}
