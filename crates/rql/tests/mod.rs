// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{error::Error, fmt::Write, path::Path};

use reifydb_catalog::{
	CatalogStore,
	catalog::Catalog,
	store::{namespace::create::NamespaceToCreate, table::create::TableToCreate},
};
use reifydb_engine::test_utils::create_test_command_transaction;
use reifydb_rql::explain::{
	ast::explain_ast, logical::explain_logical_plan, physical::explain_physical_plan, tokenize::explain_tokenize,
};
use reifydb_testing::testscript::{
	command::Command,
	runner::{Runner, run_path},
};
use test_each_file::test_each_path;

test_each_path! { in "crates/rql/tests/scripts/tokenize" as tokenize => run_test }
test_each_path! { in "crates/rql/tests/scripts/ast" as ast => run_test }
test_each_path! { in "crates/rql/tests/scripts/logical_plan" as logical_plan => run_test }
test_each_path! { in "crates/rql/tests/scripts/physical_plan" as physical_plan => run_test }

// Flow tests
test_each_path! { in "crates/rql/tests/scripts/tokenize/flow" as tokenize_flow => run_test }
test_each_path! { in "crates/rql/tests/scripts/ast/flow" as ast_flow => run_test }
test_each_path! { in "crates/rql/tests/scripts/logical_plan/flow" as logical_plan_flow => run_test }
test_each_path! { in "crates/rql/tests/scripts/physical_plan/flow" as physical_plan_flow => run_test }

fn run_test(path: &Path) {
	run_path(&mut TestRunner {}, path).expect("test failed")
}

pub struct TestRunner {}

impl Runner for TestRunner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
		let mut output = String::new();
		match command.name.as_str() {
			// token QUERY
			"tokenize" => {
				let mut args = command.consume_args();
				let query = args.next_pos().ok_or("args not given")?.value.as_str();
				args.reject_rest()?;
				let result = explain_tokenize(query).unwrap();
				writeln!(output, "{}", result).unwrap();
			}
			// ast QUERY
			"ast" => {
				let mut args = command.consume_args();
				let query = args.next_pos().ok_or("args not given")?.value.as_str();
				args.reject_rest()?;
				let result = explain_ast(query).unwrap();
				writeln!(output, "{}", result).unwrap();
			}
			// logical QUERY
			"logical" => {
				let mut args = command.consume_args();
				let query = args.next_pos().ok_or("args not given")?.value.as_str();
				args.reject_rest()?;

				let rt = tokio::runtime::Runtime::new().unwrap();
				let result = rt.block_on(async {
					let mut dummy_tx = create_test_command_transaction();

					let default_namespace = CatalogStore::create_namespace(
						&mut dummy_tx,
						NamespaceToCreate {
							namespace_fragment: None,
							name: "default".to_string(),
						},
					)
					.unwrap();

					CatalogStore::create_table(
						&mut dummy_tx,
						TableToCreate {
							fragment: None,
							table: "users".to_string(),
							namespace: default_namespace.id,
							columns: vec![],
							retention_policy: None,
						},
					)
					.unwrap();

					CatalogStore::create_table(
						&mut dummy_tx,
						TableToCreate {
							fragment: None,
							table: "orders".to_string(),
							namespace: default_namespace.id,
							columns: vec![],
							retention_policy: None,
						},
					)
					.unwrap();

					// Also create test namespace for tests that
					// explicitly use test.users
					let test_ns = CatalogStore::create_namespace(
						&mut dummy_tx,
						NamespaceToCreate {
							namespace_fragment: None,
							name: "test".to_string(),
						},
					)
					.unwrap();

					CatalogStore::create_table(
						&mut dummy_tx,
						TableToCreate {
							fragment: None,
							table: "users".to_string(),
							namespace: test_ns.id,
							columns: vec![],
							retention_policy: None,
						},
					)
					.unwrap();

					let catalog = Catalog::testing();
					explain_logical_plan(&catalog, &mut (&mut dummy_tx).into(), query).unwrap()
				});
				writeln!(output, "{}", result).unwrap();
			}
			// physical QUERY
			"physical" => {
				let mut args = command.consume_args();
				let query = args.next_pos().ok_or("args not given")?.value.as_str();
				args.reject_rest()?;

				let rt = tokio::runtime::Runtime::new().unwrap();
				let result = rt.block_on(async {
					let mut dummy_tx = create_test_command_transaction();

					let namespace = CatalogStore::create_namespace(
						&mut dummy_tx,
						NamespaceToCreate {
							namespace_fragment: None,
							name: "default".to_string(),
						},
					)
					.unwrap();

					CatalogStore::create_table(
						&mut dummy_tx,
						TableToCreate {
							fragment: None,
							table: "users".to_string(),
							namespace: namespace.id,
							columns: vec![],
							retention_policy: None,
						},
					)
					.unwrap();

					CatalogStore::create_table(
						&mut dummy_tx,
						TableToCreate {
							fragment: None,
							table: "orders".to_string(),
							namespace: namespace.id,
							columns: vec![],
							retention_policy: None,
						},
					)
					.unwrap();

					let catalog = Catalog::testing();
					explain_physical_plan(&catalog, &mut dummy_tx, query).unwrap()
				});
				writeln!(output, "{}", result).unwrap();
			}
			_ => unimplemented!(),
		}
		Ok(output)
	}
}
