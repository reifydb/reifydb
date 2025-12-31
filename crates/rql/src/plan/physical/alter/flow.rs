// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_transaction::IntoStandardTransaction;
use reifydb_type::Fragment;

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
	pub(crate) async fn compile_alter_flow<T: IntoStandardTransaction>(
		&self,
		rx: &mut T,
		alter: logical::alter::AlterFlowNode,
	) -> crate::Result<PhysicalPlan> {
		let action = match alter.action {
			logical::alter::AlterFlowAction::Rename {
				new_name,
			} => AlterFlowAction::Rename {
				new_name,
			},
			logical::alter::AlterFlowAction::SetQuery {
				query,
			} => {
				// Compile logical plans to physical plans
				let physical_query = Box::pin(self.compile(rx, query)).await?.map(Box::new).unwrap();
				AlterFlowAction::SetQuery {
					query: physical_query,
				}
			}
			logical::alter::AlterFlowAction::Pause => AlterFlowAction::Pause,
			logical::alter::AlterFlowAction::Resume => AlterFlowAction::Resume,
		};

		let plan = AlterFlowNode {
			flow: alter.flow,
			action,
		};
		Ok(PhysicalPlan::AlterFlow(plan))
	}
}
