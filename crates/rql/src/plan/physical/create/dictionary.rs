// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::diagnostic::catalog::namespace_not_found;
use reifydb_transaction::IntoStandardTransaction;
use reifydb_type::{Fragment, return_error};

use crate::{
	convert_data_type,
	plan::{
		logical,
		physical::{Compiler, CreateDictionaryNode, PhysicalPlan},
	},
};

impl Compiler {
	pub(crate) async fn compile_create_dictionary<T: IntoStandardTransaction>(
		&self,
		rx: &mut T,
		create: logical::CreateDictionaryNode,
	) -> crate::Result<PhysicalPlan> {
		// Get namespace name from the MaybeQualified type
		let namespace_name = create.dictionary.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
		let Some(namespace_def) = self.catalog.find_namespace_by_name(rx, namespace_name).await? else {
			let ns_fragment = create
				.dictionary
				.namespace
				.clone()
				.unwrap_or_else(|| Fragment::internal("default".to_string()));
			return_error!(namespace_not_found(ns_fragment, namespace_name));
		};

		// Convert AstDataType to Type
		let value_type = match &create.value_type {
			crate::ast::AstDataType::Unconstrained(name) => convert_data_type(name)?,
			crate::ast::AstDataType::Constrained {
				name,
				..
			} => convert_data_type(name)?,
		};

		let id_type = match &create.id_type {
			crate::ast::AstDataType::Unconstrained(name) => convert_data_type(name)?,
			crate::ast::AstDataType::Constrained {
				name,
				..
			} => convert_data_type(name)?,
		};

		Ok(PhysicalPlan::CreateDictionary(CreateDictionaryNode {
			namespace: namespace_def,
			dictionary: create.dictionary.name.clone(),
			if_not_exists: create.if_not_exists,
			value_type,
			id_type,
		}))
	}
}
