// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::property::{ColumnPropertyKind, ColumnSaturationStrategy};
use reifydb_transaction::transaction::Transaction;

use crate::{
	Result,
	ast::ast::{AstColumnPropertyKind, AstCreateColumnProperty},
	plan::logical::{Compiler, CreateColumnPropertyNode, LogicalPlan},
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
				AstColumnPropertyKind::Saturation => {
					if entry.value.is_literal_none() {
						ColumnPropertyKind::Saturation(ColumnSaturationStrategy::None)
					} else {
						let ident = entry.value.as_identifier().text();
						match ident {
							"error" => ColumnPropertyKind::Saturation(
								ColumnSaturationStrategy::Error,
							),
							_ => unimplemented!(),
						}
					}
				}
				AstColumnPropertyKind::Default => unimplemented!(),
			})
			.collect();

		Ok(LogicalPlan::CreateColumnProperty(CreateColumnPropertyNode {
			column: ast.column,
			properties,
		}))
	}
}
