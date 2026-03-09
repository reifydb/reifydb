// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::{columns::Columns, headers::ColumnHeaders};
use reifydb_rql::nodes::RunTestsNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::params::Params;
use reifydb_type::value::identity::IdentityId;

use crate::{
	Result,
	test::run::run_tests,
	vm::{
		services::Services,
		stack::SymbolTable,
		vm::Vm,
		volcano::query::{QueryContext, QueryNode},
	},
};

pub(crate) struct RunTestsQueryNode {
	node: RunTestsNode,
	services: Arc<Services>,
	stack: SymbolTable,
	identity: IdentityId,
	executed: bool,
}

impl RunTestsQueryNode {
	pub fn new(node: RunTestsNode, context: Arc<QueryContext>) -> Self {
		Self {
			node,
			services: context.services.clone(),
			stack: context.stack.clone(),
			identity: context.identity,
			executed: false,
		}
	}
}

impl QueryNode for RunTestsQueryNode {
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		Ok(())
	}

	fn next<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		if self.executed {
			return Ok(None);
		}
		self.executed = true;

		let mut vm = Vm::new(self.stack.clone(), self.identity);
		vm.in_test_context = true;
		let columns = run_tests(&mut vm, &self.services, rx, self.node.clone(), &Params::None)?;
		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		None
	}
}
