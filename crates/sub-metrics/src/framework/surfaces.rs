// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::id::NamespaceId;
use reifydb_engine::engine::StandardEngine;
use reifydb_value::Result;

use crate::framework::{
	accumulator::PublishedSurface,
	current::{CurrentCache, CurrentVTable},
	spec::{DomainSpec, MetricsDomain, Surface},
};

pub struct MetricsSurfaces {
	entries: Vec<SurfaceEntry>,
}

struct SurfaceEntry {
	domain: MetricsDomain,
	surface: Surface,
	namespace: NamespaceId,
	cache: CurrentCache,
}

impl MetricsSurfaces {
	pub fn build(specs: impl IntoIterator<Item = DomainSpec>) -> Self {
		let mut entries = Vec::new();
		for spec in specs {
			entries.push(SurfaceEntry {
				domain: spec.domain,
				surface: Surface::Current,
				namespace: spec.namespace,
				cache: CurrentCache::new(spec.columns(Surface::Current)),
			});
			if spec.has_total {
				entries.push(SurfaceEntry {
					domain: spec.domain,
					surface: Surface::Total,
					namespace: spec.namespace,
					cache: CurrentCache::new(spec.columns(Surface::Total)),
				});
			}
		}
		Self {
			entries,
		}
	}

	pub fn cache(&self, domain: MetricsDomain, surface: Surface) -> Option<&CurrentCache> {
		self.entries.iter().find(|e| e.domain == domain && e.surface == surface).map(|e| &e.cache)
	}

	pub fn store(&self, published: PublishedSurface) {
		if let Some(cache) = self.cache(published.domain, published.surface) {
			cache.store(published.columns);
		}
	}

	pub fn register_all(&self, engine: &StandardEngine) -> Result<()> {
		for entry in &self.entries {
			if entry.domain == MetricsDomain::ProfilerSpans {
				continue;
			}
			engine.register_virtual_table(
				entry.namespace,
				entry.surface.table_name(),
				CurrentVTable::new(entry.cache.clone()),
			)?;
		}
		Ok(())
	}
}
