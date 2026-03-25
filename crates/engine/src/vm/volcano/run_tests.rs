// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::{columns::Columns, headers::ColumnHeaders};
use reifydb_rql::nodes::RunTestsNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::params::Params;

use crate::{
	Result,
	run_tests::run::run_tests,
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
	symbols: SymbolTable,
	executed: bool,
}

impl RunTestsQueryNode {
	pub fn new(node: RunTestsNode, context: Arc<QueryContext>) -> Self {
		Self {
			node,
			services: context.services.clone(),
			symbols: context.symbols.clone(),
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

		let mut vm = Vm::new(self.symbols.clone());
		let columns = run_tests(&mut vm, &self.services, rx, self.node.clone(), &Params::None)?;
		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		None
	}
}
