// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::standard::IntoStandardTransaction;
use reifydb_type::fragment::Fragment;

use crate::{
	ast::identifier::MaybeQualifiedFlowIdentifier,
	plan::{
		logical,
		physical::{Compiler, PhysicalPlan},
	},
};

#[derive(Debug, Clone)]
pub struct AlterFlowNode {
	pub flow: MaybeQualifiedFlowIdentifier,
	pub action: AlterFlowAction,
}

#[derive(Debug, Clone)]
pub enum AlterFlowAction {
	Rename {
		new_name: Fragment,
	},
	SetQuery {
		query: Box<PhysicalPlan>,
	},
	Pause,
	Resume,
}

impl Compiler {
	pub(crate) fn compile_alter_flow<T: IntoStandardTransaction>(
		&self,
		rx: &mut T,
		alter: logical::alter::flow::AlterFlowNode,
	) -> crate::Result<PhysicalPlan> {
		let action = match alter.action {
			logical::alter::flow::AlterFlowAction::Rename {
				new_name,
			} => AlterFlowAction::Rename {
				new_name,
			},
			logical::alter::flow::AlterFlowAction::SetQuery {
				query,
			} => {
				// Compile logical plans to physical plans
				let physical_query = self.compile(rx, query)?.map(Box::new).unwrap();
				AlterFlowAction::SetQuery {
					query: physical_query,
				}
			}
			logical::alter::flow::AlterFlowAction::Pause => AlterFlowAction::Pause,
			logical::alter::flow::AlterFlowAction::Resume => AlterFlowAction::Resume,
		};

		let plan = AlterFlowNode {
			flow: alter.flow,
			action,
		};
		Ok(PhysicalPlan::AlterFlow(plan))
	}
}
