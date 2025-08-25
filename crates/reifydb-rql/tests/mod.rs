// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{error::Error, fmt::Write, path::Path};

use reifydb_catalog::{
	CatalogStore, schema::SchemaToCreate, table::TableToCreate,
};
use reifydb_engine::test_utils::create_test_command_transaction;
use reifydb_rql::explain::{
	explain_ast, explain_lex, explain_logical_plan, explain_physical_plan,
};
use reifydb_testing::{testscript, testscript::Command};
use test_each_file::test_each_path;

test_each_path! { in "crates/reifydb-rql/tests/scripts/lex" as lex => run_test }
test_each_path! { in "crates/reifydb-rql/tests/scripts/ast" as ast => run_test }
test_each_path! { in "crates/reifydb-rql/tests/scripts/logical_plan" as logical_plan => run_test }
test_each_path! { in "crates/reifydb-rql/tests/scripts/physical_plan" as physical_plan => run_test }

fn run_test(path: &Path) {
	testscript::run_path(&mut Runner {}, path).expect("test failed")
}

pub struct Runner {}

impl testscript::Runner for Runner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
		let mut output = String::new();
		match command.name.as_str() {
			// lex QUERY
			"lex" => {
				let mut args = command.consume_args();
				let query = args
					.next_pos()
					.ok_or("args not given")?
					.value
					.as_str();
				args.reject_rest()?;
				let result = explain_lex(query).unwrap();
				writeln!(output, "{}", result).unwrap();
			}
			// ast QUERY
			"ast" => {
				let mut args = command.consume_args();
				let query = args
					.next_pos()
					.ok_or("args not given")?
					.value
					.as_str();
				args.reject_rest()?;
				let result = explain_ast(query).unwrap();
				writeln!(output, "{}", result).unwrap();
			}
			// logical QUERY
			"logical" => {
				let mut args = command.consume_args();
				let query = args
					.next_pos()
					.ok_or("args not given")?
					.value
					.as_str();
				args.reject_rest()?;
				let result =
					explain_logical_plan(query).unwrap();
				writeln!(output, "{}", result).unwrap();
			}
			// physical QUERY
			"physical" => {
				let mut args = command.consume_args();
				let query = args
					.next_pos()
					.ok_or("args not given")?
					.value
					.as_str();
				args.reject_rest()?;

				let mut dummy_tx =
					create_test_command_transaction();

				CatalogStore::create_schema(
					&mut dummy_tx,
					SchemaToCreate {
						schema_fragment: None,
						name: "default".to_string(),
					},
				)
				.unwrap();

				CatalogStore::create_table(
					&mut dummy_tx,
					TableToCreate {
						fragment: None,
						table: "users".to_string(),
						schema: "default".to_string(),
						columns: vec![],
					},
				)
				.unwrap();

				CatalogStore::create_table(
					&mut dummy_tx,
					TableToCreate {
						fragment: None,
						table: "orders".to_string(),
						schema: "default".to_string(),
						columns: vec![],
					},
				)
				.unwrap();

				let result = explain_physical_plan(
					&mut dummy_tx,
					query,
				)
				.unwrap();
				writeln!(output, "{}", result).unwrap();
			}
			_ => unimplemented!(),
		}
		Ok(output)
	}
}
