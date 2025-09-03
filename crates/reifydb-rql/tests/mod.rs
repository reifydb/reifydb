// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{error::Error, fmt::Write, path::Path};

use reifydb_catalog::{
    schema::SchemaToCreate, table::TableToCreate, CatalogStore,
};
use reifydb_engine::test_utils::create_test_command_transaction;
use reifydb_rql::explain::{
    explain_ast, explain_logical_plan, explain_physical_plan,
    explain_tokenize,
};
use reifydb_testing::{testscript, testscript::Command};

pub fn run_test(path: &Path) {
	testscript::run_path(&mut Runner {}, path).expect("test failed")
}

pub struct Runner {}

impl testscript::Runner for Runner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
		let mut output = String::new();
		match command.name.as_str() {
			// tokenize QUERY
			"tokenize" => {
				let mut args = command.consume_args();
				let query = args
					.next_pos()
					.ok_or("args not given")?
					.value
					.as_str();
				args.reject_rest()?;
				let result = explain_tokenize(query).unwrap();
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

				let schema = CatalogStore::create_schema(
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
						schema: schema.id,
						columns: vec![],
					},
				)
				.unwrap();

				CatalogStore::create_table(
					&mut dummy_tx,
					TableToCreate {
						fragment: None,
						table: "orders".to_string(),
						schema: schema.id,
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
