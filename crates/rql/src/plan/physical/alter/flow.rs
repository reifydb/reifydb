// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::fragment::Fragment;

use crate::plan::{
	logical,
	physical::{Compiler, PhysicalPlan},
};

#[derive(Debug, Clone)]
pub struct AlterFlowNode {
	pub flow: AlterFlowIdentifier,
	pub action: AlterFlowAction,
}

#[derive(Debug, Clone)]
pub struct AlterFlowIdentifier {
	pub namespace: Option<Fragment>,
	pub name: Fragment,
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
	pub(crate) fn compile_alter_flow<T: AsTransaction>(
		&self,
		rx: &mut T,
		alter: logical::alter::flow::AlterFlowNode<'_>,
	) -> crate::Result<PhysicalPlan> {
		let flow = AlterFlowIdentifier {
			namespace: alter.flow.namespace.map(|n| n.to_owned()),
			name: alter.flow.name.to_owned(),
		};

		let action = match alter.action {
			logical::alter::flow::AlterFlowAction::Rename {
				new_name,
			} => AlterFlowAction::Rename {
				new_name: new_name.to_owned(),
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
			flow,
			action,
		};
		Ok(PhysicalPlan::AlterFlow(plan))
	}
}
