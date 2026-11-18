// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::event::{EventListener, catalog::CatalogChangedEvent};
use reifydb_rql::compiler::Compiler;

pub struct PlanCacheListener {
	compiler: Compiler,
}

impl PlanCacheListener {
	pub fn new(compiler: Compiler) -> Self {
		Self {
			compiler,
		}
	}
}

impl EventListener<CatalogChangedEvent> for PlanCacheListener {
	fn on(&self, event: &CatalogChangedEvent) {
		self.compiler.record_catalog_change(*event.version());
		self.compiler.clear();
	}
}
