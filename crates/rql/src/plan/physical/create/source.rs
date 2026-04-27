// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	nodes::{ConfigPair, CreateSourceNode},
	plan::{
		logical,
		physical::{Compiler, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_source(
		&mut self,
		rx: &mut Transaction<'_>,
		create: logical::CreateSourceNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		// Resolve source namespace
		let ns_segments: Vec<&str> = create.name.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_fragment = if let Some(n) = create.name.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(ns_segments.join("::"))
			} else {
				Fragment::internal("default")
			};
			return Err(CatalogError::NotFound {
				kind: CatalogObjectKind::Namespace,
				namespace: ns_segments.join("::"),
				name: String::new(),
				fragment: ns_fragment,
			}
			.into());
		};

		// Resolve target namespace
		let target_ns_segments: Vec<&str> = create.target.namespace.iter().map(|n| n.text()).collect();
		let Some(target_namespace) = self.catalog.find_namespace_by_segments(rx, &target_ns_segments)? else {
			let ns_fragment = if let Some(n) = create.target.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(target_ns_segments.join("::"))
			} else {
				Fragment::internal("default")
			};
			return Err(CatalogError::NotFound {
				kind: CatalogObjectKind::Namespace,
				namespace: target_ns_segments.join("::"),
				name: String::new(),
				fragment: ns_fragment,
			}
			.into());
		};

		// Resolve config pairs
		let config = create
			.config
			.iter()
			.map(|pair| ConfigPair {
				key: self.interner.intern_fragment(&pair.key),
				value: Fragment::internal(format!("{:?}", pair.value)),
			})
			.collect();

		Ok(PhysicalPlan::CreateSource(CreateSourceNode {
			namespace,
			name: self.interner.intern_fragment(&create.name.name),
			connector: self.interner.intern_fragment(&create.connector),
			config,
			target_namespace,
			target_name: self.interner.intern_fragment(&create.target.name),
		}))
	}
}
