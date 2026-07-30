// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Reclaimed {
	pub data: Vec<RetiredGroup>,

	pub keyspace: Vec<RetiredGroup>,

	pub rows: usize,

	pub backlog: usize,
}

impl Reclaimed {
	pub fn is_empty(&self) -> bool {
		self.data.is_empty() && self.keyspace.is_empty()
	}

	pub fn groups(&self) -> impl Iterator<Item = u64> + '_ {
		self.data.iter().chain(self.keyspace.iter()).map(|retired| retired.group)
	}
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetiredGroup {
	pub group: u64,

	pub cutoff_ms: u64,
}
