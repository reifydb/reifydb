// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_type::Fragment;

use crate::{
	ast::{AstAlterFlow, AstAlterFlowAction, identifier::MaybeQualifiedFlowIdentifier},
	plan::logical::{Compiler, LogicalPlan},
};

#[derive(Debug)]
pub struct AlterFlowNode {
	pub flow: MaybeQualifiedFlowIdentifier,
	pub action: AlterFlowAction,
}

#[derive(Debug)]
pub enum AlterFlowAction {
	Rename {
		new_name: Fragment,
	},
	SetQuery {
		query: Vec<LogicalPlan>,
	},
	Pause,
	Resume,
}

impl Compiler {
	pub(crate) async fn compile_alter_flow<T: CatalogQueryTransaction>(
		ast: AstAlterFlow,
		tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		let flow = ast.flow.clone();

		let action = match ast.action {
			AstAlterFlowAction::Rename {
				new_name,
			} => AlterFlowAction::Rename {
				new_name,
			},
			AstAlterFlowAction::SetQuery {
				query,
			} => {
				// Compile the query statement to logical plan
				let compiled_query = Compiler::compile(query, tx).await?;
				AlterFlowAction::SetQuery {
					query: compiled_query,
				}
			}
			AstAlterFlowAction::Pause => AlterFlowAction::Pause,
			AstAlterFlowAction::Resume => AlterFlowAction::Resume,
		};

		let node = AlterFlowNode {
			flow,
			action,
		};
		Ok(LogicalPlan::AlterFlow(node))
	}
}
