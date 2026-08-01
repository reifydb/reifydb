// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod collect;

use collect::{Collectors, collect_memory, collect_operators, collect_watermarks};
use reifydb_core::{interface::catalog::id::NamespaceId, metrics::sample::MetricsSample};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Domain {
	Memory,
	Watermarks,
	Operators,
}

impl Domain {
	pub const ALL: [Domain; 3] = [Domain::Memory, Domain::Watermarks, Domain::Operators];

	pub fn namespace(&self) -> NamespaceId {
		match self {
			Domain::Memory => NamespaceId::SYSTEM_METRICS_RUNTIME_MEMORY,
			Domain::Watermarks => NamespaceId::SYSTEM_METRICS_RUNTIME_WATERMARKS,
			Domain::Operators => NamespaceId::SYSTEM_METRICS_RUNTIME_OPERATORS,
		}
	}

	pub fn local_name(&self) -> &'static str {
		match self {
			Domain::Memory => "memory",
			Domain::Watermarks => "watermarks",
			Domain::Operators => "operators",
		}
	}

	pub fn collect(&self, c: &Collectors) -> Vec<MetricsSample> {
		match self {
			Domain::Memory => collect_memory(c),
			Domain::Watermarks => collect_watermarks(c),
			Domain::Operators => collect_operators(c),
		}
	}
}

#[derive(Clone)]
pub struct SampleReader {
	collectors: Collectors,
}

impl SampleReader {
	pub fn new(collectors: Collectors) -> Self {
		Self {
			collectors,
		}
	}

	pub fn samples_for(&self, domain: Domain) -> Vec<MetricsSample> {
		domain.collect(&self.collectors)
	}
}
