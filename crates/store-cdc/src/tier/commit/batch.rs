// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{common::CommitVersion, interface::cdc::Cdc};
use reifydb_value::byte_size::ByteSize;

#[derive(Debug, Default)]
pub struct FlushBatch {
	pub entries: Vec<Arc<Cdc>>,
	pub bytes: ByteSize,
}

impl FlushBatch {
	pub(crate) fn min_version(&self) -> Option<CommitVersion> {
		self.entries.first().map(|cdc| cdc.version)
	}

	pub(crate) fn max_version(&self) -> Option<CommitVersion> {
		self.entries.last().map(|cdc| cdc.version)
	}

	pub(crate) fn get(&self, version: CommitVersion) -> Option<Arc<Cdc>> {
		self.entries
			.binary_search_by(|cdc| cdc.version.cmp(&version))
			.ok()
			.map(|index| Arc::clone(&self.entries[index]))
	}

	pub(crate) fn collect_range(&self, lo: CommitVersion, hi: CommitVersion, want: usize, out: &mut Vec<Arc<Cdc>>) {
		let start = self.entries.partition_point(|cdc| cdc.version < lo);
		for cdc in &self.entries[start..] {
			if out.len() >= want || cdc.version > hi {
				return;
			}
			out.push(Arc::clone(cdc));
		}
	}
}
