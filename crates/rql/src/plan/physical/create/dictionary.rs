// SPDX-License-Identifier: AGPL-3.0-or-later
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
		let namespace_name = if create.dictionary.namespace.is_empty() {
			"default".to_string()
		} else {
			create.dictionary.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join("::")
		};
		let Some(namespace_def) = self.catalog.find_namespace_by_name(rx, &namespace_name)? else {
			let ns_fragment = if let Some(n) = create.dictionary.namespace.first() {
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

		let value_type = convert_data_type_with_constraints(&create.value_type)?.get_type();
		let id_type = convert_data_type_with_constraints(&create.id_type)?.get_type();

		Ok(PhysicalPlan::CreateDictionary(CreateDictionaryNode {
			namespace: namespace_def,
			dictionary: self.interner.intern_fragment(&create.dictionary.name),
			if_not_exists: create.if_not_exists,
			value_type,
			id_type,
		}))
	}
}
