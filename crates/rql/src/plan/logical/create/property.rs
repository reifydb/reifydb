// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_transaction::transaction::Transaction;

use crate::{
	Result,
	ast::ast::{AstColumnPropertyKind, AstCreateColumnProperty},
	plan::logical::{
		Compiler, CreateColumnPropertyNode, LogicalPlan,
		create::{column_saturation_property, reject_column_default},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_column_property(
		&self,
		ast: AstCreateColumnProperty<'bump>,
		_tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let properties = ast
			.properties
			.iter()
			.map(|entry| match entry.kind {
				AstColumnPropertyKind::Saturation => column_saturation_property(&entry.value),
				AstColumnPropertyKind::Default => Err(reject_column_default(&entry.value)),
			})
			.collect::<Result<Vec<_>>>()?;

		Ok(LogicalPlan::CreateColumnProperty(CreateColumnPropertyNode {
			column: ast.column,
			properties,
		}))
	}
}
