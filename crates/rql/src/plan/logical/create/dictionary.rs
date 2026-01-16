// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{
	ast::ast::AstCreateDictionary,
	plan::logical::{Compiler, CreateDictionaryNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_dictionary(&self, ast: AstCreateDictionary) -> crate::Result<LogicalPlan> {
		Ok(LogicalPlan::CreateDictionary(CreateDictionaryNode {
			dictionary: ast.dictionary,
			if_not_exists: ast.if_not_exists,
			value_type: ast.value_type,
			id_type: ast.id_type,
		}))
	}
}
