// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::id::NamespaceId;

use crate::collect::{Collectors, Sample, collect_memory, collect_watermarks};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Domain {
	Memory,
	Watermarks,
}

impl Domain {
	pub const ALL: [Domain; 2] = [Domain::Memory, Domain::Watermarks];

	pub fn namespace(&self) -> NamespaceId {
		match self {
			Domain::Memory => NamespaceId::SYSTEM_METRICS_RUNTIME_MEMORY,
			Domain::Watermarks => NamespaceId::SYSTEM_METRICS_RUNTIME_WATERMARKS,
		}
	}

	pub fn local_name(&self) -> &'static str {
		match self {
			Domain::Memory => "memory",
			Domain::Watermarks => "watermarks",
		}
	}

	pub fn snapshots_path(&self) -> &'static str {
		match self {
			Domain::Memory => "system::metrics::runtime::memory::snapshots",
			Domain::Watermarks => "system::metrics::runtime::watermarks::snapshots",
		}
	}

	pub fn collect(&self, c: &Collectors) -> Vec<Sample> {
		match self {
			Domain::Memory => collect_memory(c),
			Domain::Watermarks => collect_watermarks(c),
		}
	}
}
