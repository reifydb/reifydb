// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result, convert_data_type_with_constraints,
	nodes::{CreateSumTypeColumn, CreateSumTypeNode, CreateSumTypeVariant},
	plan::{
		logical,
		physical::{Compiler, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_sumtype(
		&mut self,
		rx: &mut Transaction<'_>,
		create: logical::CreateSumTypeNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
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

		let mut variants = Vec::with_capacity(create.variants.len());
		for variant in create.variants {
			let mut columns = Vec::with_capacity(variant.columns.len());
			for col in variant.columns {
				let type_constraint = convert_data_type_with_constraints(&col.ty)?;
				columns.push(CreateSumTypeColumn {
					name: col.name.text().to_string(),
					column_type: type_constraint,
				});
			}
			variants.push(CreateSumTypeVariant {
				name: variant.name.text().to_string(),
				columns,
			});
		}

		Ok(PhysicalPlan::CreateSumType(CreateSumTypeNode {
			namespace,
			name: self.interner.intern_fragment(&create.name.name),
			if_not_exists: create.if_not_exists,
			variants,
		}))
	}
}
