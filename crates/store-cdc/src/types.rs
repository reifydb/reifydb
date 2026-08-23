// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{common::CommitVersion, event::metric::CdcEviction, interface::cdc::Cdc};
use reifydb_value::{byte_size::ByteSize, count::Count, value::datetime::DateTime};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockId(pub CommitVersion);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockSummary {
	pub id: BlockId,
	pub min_version: CommitVersion,
	pub max_version: CommitVersion,
	pub min_timestamp: DateTime,
	pub max_timestamp: DateTime,
	pub count: Count,
	pub stored_bytes: ByteSize,
}

#[derive(Debug, Clone)]
pub struct Block {
	pub summary: BlockSummary,
	pub entries: Vec<Arc<Cdc>>,
}

impl Block {
	pub fn id(&self) -> BlockId {
		self.summary.id
	}

	pub fn min_version(&self) -> CommitVersion {
		self.summary.min_version
	}

	pub fn max_version(&self) -> CommitVersion {
		self.summary.max_version
	}

	pub fn contains(&self, version: CommitVersion) -> bool {
		version >= self.summary.min_version && version <= self.summary.max_version
	}

	pub fn resident_bytes(&self) -> ByteSize {
		self.entries.iter().fold(ByteSize::ZERO, |acc, cdc| acc.saturating_add(cdc_resident_bytes(cdc)))
	}
}

pub fn cdc_resident_bytes(cdc: &Cdc) -> ByteSize {
	let payload: usize = cdc.changes.iter().map(|change| change.key().len() + change.value_bytes()).sum();
	ByteSize::from_bytes((size_of::<Cdc>() + payload) as u64)
}

#[derive(Debug, Clone, Default)]
pub struct DropOutcome {
	pub count: Count,
	pub entries: Vec<CdcEviction>,
	pub more_remaining: bool,
}
