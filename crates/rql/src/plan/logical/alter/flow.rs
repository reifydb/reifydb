// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::Transaction;

use crate::{
	ast::{
		ast::{AstAlterFlow, AstAlterFlowAction},
		identifier::MaybeQualifiedFlowIdentifier,
	},
	bump::{BumpFragment, BumpVec},
	plan::logical::{Compiler, LogicalPlan},
};

#[derive(Debug)]
pub struct AlterFlowNode<'bump> {
	pub flow: MaybeQualifiedFlowIdentifier<'bump>,
	pub action: AlterFlowAction<'bump>,
}

#[derive(Debug)]
pub enum AlterFlowAction<'bump> {
	Rename {
		new_name: BumpFragment<'bump>,
	},
	SetQuery {
		query: BumpVec<'bump, LogicalPlan<'bump>>,
	},
	Pause,
	Resume,
}

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_alter_flow(
		&self,
		ast: AstAlterFlow<'bump>,
		tx: &mut Transaction<'_>,
	) -> crate::Result<LogicalPlan<'bump>> {
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
