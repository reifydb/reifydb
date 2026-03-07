// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::{
	Result,
	ast::ast::AstCreateDictionary,
	plan::logical::{Compiler, CreateDictionaryNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_dictionary(&self, ast: AstCreateDictionary<'bump>) -> Result<LogicalPlan<'bump>> {
		Ok(LogicalPlan::CreateDictionary(CreateDictionaryNode {
			dictionary: ast.dictionary,
			if_not_exists: ast.if_not_exists,
			value_type: ast.value_type,
			id_type: ast.id_type,
		}))
	}
}
