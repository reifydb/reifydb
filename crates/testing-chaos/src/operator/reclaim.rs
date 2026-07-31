// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Reclaimed {
	pub data: Vec<RetiredGroup>,

	pub identity: Vec<RetiredGroup>,

	pub keyspace: Vec<RetiredGroup>,

	pub mapping_rows: usize,

	pub rows: usize,

	pub backlog: usize,
}

impl Reclaimed {
	pub fn is_empty(&self) -> bool {
		self.data.is_empty() && self.identity.is_empty() && self.keyspace.is_empty() && self.mapping_rows == 0
	}

	pub fn groups(&self) -> impl Iterator<Item = u64> + '_ {
		self.data.iter().chain(self.identity.iter()).chain(self.keyspace.iter()).map(|retired| retired.group)
	}
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReclaimTally {
	pub sweeps: usize,

	pub groups: usize,

	pub rows: usize,
}

impl ReclaimTally {
	pub fn record(&mut self, reclaimed: &Reclaimed) {
		self.sweeps += 1;
		self.groups += reclaimed.groups().count();
		self.rows += reclaimed.rows;
	}

	pub fn reclaimed_nothing(&self) -> bool {
		self.groups == 0 && self.rows == 0
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
