// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::fragment::Fragment;

use crate::{
	ast::{
		ast::{AstAlterFlow, AstAlterFlowAction},
		identifier::MaybeQualifiedFlowIdentifier,
	},
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
	pub(crate) fn compile_alter_flow<T: AsTransaction>(
		&self,
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
				let compiled_query = self.compile(query, tx)?;
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
