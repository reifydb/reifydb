// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::Transaction;

use crate::{
	nodes::AlterFlowIdentifier,
	plan::{
		logical,
		physical::{AlterFlowAction, AlterFlowNode, Compiler, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_alter_flow(
		&mut self,
		rx: &mut Transaction<'_>,
		alter: logical::alter::flow::AlterFlowNode<'bump>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		let flow = AlterFlowIdentifier {
			namespace: alter.flow.namespace.first().map(|n| self.interner.intern_fragment(n)),
			name: self.interner.intern_fragment(&alter.flow.name),
		};

		let action = match alter.action {
			logical::alter::flow::AlterFlowAction::Rename {
				new_name,
			} => AlterFlowAction::Rename {
				new_name: self.interner.intern_fragment(&new_name),
			},
			logical::alter::flow::AlterFlowAction::SetQuery {
				query,
			} => {
				// Compile logical plans to physical plans
				let physical_query = self.compile(rx, query)?.unwrap();
				AlterFlowAction::SetQuery {
					query: self.bump_box(physical_query),
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
