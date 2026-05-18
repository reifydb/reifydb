// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result, convert_data_type_with_constraints,
	plan::{
		logical,
		physical::{Compiler, CreateDictionaryNode, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_dictionary(
		&mut self,
		rx: &mut Transaction<'_>,
		create: logical::CreateDictionaryNode<'_>,
	) -> Result<PhysicalPlan<'bump>> {
		let ns_segments: Vec<&str> = create.dictionary.namespace.iter().map(|n| n.text()).collect();
		let Some(namespace) = self.catalog.find_namespace_by_segments(rx, &ns_segments)? else {
			let ns_fragment = if let Some(n) = create.dictionary.namespace.first() {
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

		let value_type = convert_data_type_with_constraints(&create.value_type)?.get_type();
		let id_type = convert_data_type_with_constraints(&create.id_type)?.get_type();

		Ok(PhysicalPlan::CreateDictionary(CreateDictionaryNode {
			namespace,
			dictionary: self.interner.intern_fragment(&create.dictionary.name),
			if_not_exists: create.if_not_exists,
			value_type,
			id_type,
		}))
	}
}
