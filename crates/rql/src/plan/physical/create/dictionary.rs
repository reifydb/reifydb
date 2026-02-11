// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::error::diagnostic::catalog::namespace_not_found;
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::{
	convert_data_type,
	plan::{
		logical,
		physical::{Compiler, CreateDictionaryNode, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_dictionary<T: AsTransaction>(
		&mut self,
		rx: &mut T,
		create: logical::CreateDictionaryNode<'_>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		// Get namespace name from the MaybeQualified type (join all segments for nested namespaces)
		let namespace_name = if create.dictionary.namespace.is_empty() {
			"default".to_string()
		} else {
			create.dictionary.namespace.iter().map(|n| n.text()).collect::<Vec<_>>().join(".")
		};
		let Some(namespace_def) = self.catalog.find_namespace_by_name(rx, &namespace_name)? else {
			let ns_fragment = if let Some(n) = create.dictionary.namespace.first() {
				let interned = self.interner.intern_fragment(n);
				interned.with_text(&namespace_name)
			} else {
				Fragment::internal("default".to_string())
			};
			return_error!(namespace_not_found(ns_fragment, &namespace_name));
		};

		// Convert AstDataType to Type
		let value_type = match &create.value_type {
			crate::ast::ast::AstType::Unconstrained(name) => convert_data_type(name)?,
			crate::ast::ast::AstType::Constrained {
				name,
				..
			} => convert_data_type(name)?,
		};

		let id_type = match &create.id_type {
			crate::ast::ast::AstType::Unconstrained(name) => convert_data_type(name)?,
			crate::ast::ast::AstType::Constrained {
				name,
				..
			} => convert_data_type(name)?,
		};

		Ok(PhysicalPlan::CreateDictionary(CreateDictionaryNode {
			namespace: namespace_def,
			dictionary: self.interner.intern_fragment(&create.dictionary.name),
			if_not_exists: create.if_not_exists,
			value_type,
			id_type,
		}))
	}
}
