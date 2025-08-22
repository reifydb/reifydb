// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{CommandTransaction, Transaction, ViewDef};
use reifydb_rql::plan::physical::PhysicalPlan;

use crate::execute::Executor;

#[allow(dead_code)] // FIXME
mod deferred;
mod schema;
mod table;
#[allow(dead_code)] // FIXME
mod transactional;

impl<T: Transaction> Executor<T> {
	pub(crate) fn create_flow(
		&self,
		_txn: &mut CommandTransaction<T>,
		_view: &ViewDef,
		plan: Option<Box<PhysicalPlan>>,
	) -> crate::Result<()> {
		let Some(_plan) = plan else {
			return Ok(());
		};

		// let flow = compile_flow(txn, *plan, view).unwrap();
		//
		// let rql = r#"
		//          from[{data: blob::utf8('$REPLACE')}]
		//          insert reifydb.flows
		//      "#
		// .replace(
		// 	"$REPLACE",
		// 	serde_json::to_string(&flow).unwrap().as_str(),
		// );
		//
		// self.execute_command(
		// 	txn,
		// 	Command {
		// 		rql: rql.as_str(),
		// 		params: Params::default(),
		// 		identity: &Identity::root(),
		// 	},
		// )?;

		todo!()
	}
}
