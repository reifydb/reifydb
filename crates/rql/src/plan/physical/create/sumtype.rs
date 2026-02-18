// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::error::diagnostic::catalog::namespace_not_found;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::{
	convert_data_type_with_constraints,
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
	) -> crate::Result<PhysicalPlan<'bump>> {
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
			return_error!(namespace_not_found(ns_fragment, &namespace_name));
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
			namespace: namespace_def,
			name: self.interner.intern_fragment(&create.name.name),
			if_not_exists: create.if_not_exists,
			variants,
		}))
	}
}
