// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstCreateDictionary,
	plan::logical::{Compiler, CreateDictionaryNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_dictionary<'a, T: CatalogQueryTransaction>(
		ast: AstCreateDictionary,
		_tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		Ok(LogicalPlan::CreateDictionary(CreateDictionaryNode {
			dictionary: ast.dictionary,
			if_not_exists: ast.if_not_exists,
			value_type: ast.value_type,
			id_type: ast.id_type,
		}))
	}
}
