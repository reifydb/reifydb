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
		// Get namespace name from the MaybeQualified type
		let namespace_name = create.dictionary.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
		let Some(namespace_def) = self.catalog.find_namespace_by_name(rx, namespace_name)? else {
			let ns_fragment = match create.dictionary.namespace {
				Some(n) => self.interner.intern_fragment(&n),
				None => Fragment::internal("default".to_string()),
			};
			return_error!(namespace_not_found(ns_fragment, namespace_name));
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
