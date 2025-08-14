// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	ActiveCommandTransaction, Command, ExecuteCommand, Params, Identity,
	Transaction,
};
use reifydb_rql::{
	ast,
	plan::{logical::compile_logical, physical::CreateComputedViewPlan},
};

use crate::{
	columnar::Columns, execute::Executor, flow::compile::compile_to_flow,
};

impl<T: Transaction> Executor<T> {
	pub(crate) fn create_computed_view(
		&self,
		txn: &mut ActiveCommandTransaction<T>,
		_plan: CreateComputedViewPlan,
	) -> crate::Result<Columns> {
		let rql = r#"
    create computed view test.adults { name: utf8, age: int1 }  with {
        from test.users
        filter { age > 18  }
        map { name, age }
    }"#;

		let ast_statements = match ast::parse(rql) {
			Ok(statements) => statements,
			Err(e) => {
				panic!("RQL parsing failed: {}", e);
			}
		};

		println!("AST statements: {} nodes", ast_statements.len());

		let logical_plans = match compile_logical(
			ast_statements.into_iter().next().unwrap(),
		) {
			Ok(plans) => plans,
			Err(e) => {
				panic!(
					"Logical plan compilation failed: {}",
					e
				);
			}
		};

		// Compile logical plans to FlowGraph
		let flow = compile_to_flow(logical_plans).unwrap();
		// dbg!(&flow);

		// txn.command_as_root(
		//     r#"
		//     from[{data: blob::utf8('$REPLACE')}]
		//     insert reifydb.flows
		// "#
		//     .replace("$REPLACE",
		// serde_json::to_string(&flow).unwrap().as_str())
		//     .as_str(),
		//     Params::None,
		// )
		// .unwrap();

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

		Ok(Columns::empty())
	}
}
