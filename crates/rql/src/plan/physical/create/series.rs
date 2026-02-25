// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::{error::diagnostic::catalog::namespace_not_found, interface::resolved::ResolvedNamespace};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::{
	nodes::CreateSeriesNode,
	plan::{
		logical,
		physical::{Compiler, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_series(
		&mut self,
		rx: &mut Transaction<'_>,
		create: logical::CreateSeriesNode<'_>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		let namespace_name = if create.series.namespace.is_empty() {
			"default".to_string()
		} else {
			create.series.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join(".")
		};
		let Some(namespace_def) = self.catalog.find_namespace_by_name(rx, &namespace_name)? else {
			let ns_fragment = if let Some(n) = create.series.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(&namespace_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &namespace_name));
		};

		let namespace_id = if let Some(n) = create.series.namespace.first() {
			let interned = self.interner.intern_fragment(n);
			interned.with_text(&namespace_def.name)
		} else {
			Fragment::internal(namespace_def.name.clone())
		};
		let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);

		// Resolve optional tag type
		let tag = if let Some(tag_ident) = create.tag {
			let tag_namespace_name = if tag_ident.namespace.is_empty() {
				namespace_name.clone()
			} else {
				tag_ident.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join(".")
			};
			let Some(tag_ns) = self.catalog.find_namespace_by_name(rx, &tag_namespace_name)? else {
				let ns_fragment = if let Some(n) = tag_ident.namespace.first() {
					let interned = self.interner.intern_fragment(n);
					interned.with_text(&tag_namespace_name)
				} else {
					Fragment::internal(tag_namespace_name.clone())
				};
				return Err(CatalogError::NotFound {
					kind: CatalogObjectKind::Tag,
					namespace: tag_namespace_name,
					name: tag_ident.name.text().to_string(),
					fragment: ns_fragment,
				}
				.into());
			};

			let tag_name = tag_ident.name.text();
			let Some(sumtype) = self.catalog.find_sumtype_by_name(rx, tag_ns.id, tag_name)? else {
				return Err(CatalogError::NotFound {
					kind: CatalogObjectKind::Tag,
					namespace: tag_namespace_name,
					name: tag_name.to_string(),
					fragment: self.interner.intern_fragment(&tag_ident.name),
				}
				.into());
			};

			Some(sumtype.id)
		} else {
			None
		};

		Ok(PhysicalPlan::CreateSeries(CreateSeriesNode {
			namespace: resolved_namespace,
			series: self.interner.intern_fragment(&create.series.name),
			columns: create.columns,
			tag,
			precision: create.precision,
		}))
	}
}
