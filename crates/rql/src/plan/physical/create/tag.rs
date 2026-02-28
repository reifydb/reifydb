// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result, convert_data_type_with_constraints,
	nodes::{CreateSumTypeColumn, CreateSumTypeVariant, CreateTagNode},
	plan::{
		logical,
		physical::{Compiler, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_tag(
		&mut self,
		rx: &mut Transaction<'_>,
		create: logical::CreateTagNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		let namespace_name = if create.name.namespace.is_empty() {
			"default".to_string()
		} else {
			create.name.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join(".")
		};
		let Some(namespace_def) = self.catalog.find_namespace_by_name(rx, &namespace_name)? else {
			let ns_fragment = if let Some(n) = create.name.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(&namespace_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return Err(CatalogError::NotFound {
				kind: CatalogObjectKind::Namespace,
				namespace: namespace_name.to_string(),
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

		Ok(PhysicalPlan::CreateTag(CreateTagNode {
			namespace: namespace_def,
			name: self.interner.intern_fragment(&create.name.name),
			variants,
		}))
	}
}
