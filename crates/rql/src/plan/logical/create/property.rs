// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::property::{ColumnPropertyKind, ColumnSaturationPolicy};
use reifydb_transaction::transaction::Transaction;

use crate::{
	ast::ast::{AstColumnPropertyKind, AstCreateColumnProperty},
	plan::logical::{Compiler, CreateColumnPropertyNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_column_property(
		&self,
		ast: AstCreateColumnProperty<'bump>,
		_tx: &mut Transaction<'_>,
	) -> crate::Result<LogicalPlan<'bump>> {
		let properties = ast
			.properties
			.iter()
			.map(|entry| match entry.kind {
				AstColumnPropertyKind::Saturation => {
					if entry.value.is_literal_none() {
						ColumnPropertyKind::Saturation(ColumnSaturationPolicy::None)
					} else {
						let ident = entry.value.as_identifier().text();
						match ident {
							"error" => ColumnPropertyKind::Saturation(
								ColumnSaturationPolicy::Error,
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
