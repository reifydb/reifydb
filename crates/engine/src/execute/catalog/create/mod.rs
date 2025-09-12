// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	Command, ExecuteCommand, Identity, Params, Transaction, ViewDef,
};
use reifydb_rql::{flow::compile_flow, plan::physical::PhysicalPlan};

use crate::{StandardCommandTransaction, execute::Executor};

#[allow(dead_code)] // FIXME
mod deferred;
mod namespace;
mod table;
#[allow(dead_code)] // FIXME
mod transactional;

impl Executor {
	// FIXME
	pub(crate) fn create_flow<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		view: &ViewDef,
		plan: Box<PhysicalPlan>,
	) -> crate::Result<()> {
		let flow = compile_flow(txn, *plan, view).unwrap();
		let rql = r#"
		         from[{data: blob::utf8('$REPLACE')}]
		         insert reifydb.flows
		     "#
		.replace(
			"$REPLACE",
			serde_json::to_string(&flow).unwrap().as_str(),
		);

		self.execute_command(
			txn,
			Command {
				rql: rql.as_str(),
				params: Params::default(),
				identity: &Identity::root(),
			},
		)?;

		Ok(())
	}
}
