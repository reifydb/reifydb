// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{error::Error, fmt::Write, path::Path, sync::Arc};

use bumpalo::Bump;
use reifydb::{Database, embedded, vendor::tokio::runtime::Runtime};
use reifydb_rqlv2::{
	ast::{
		explain::explain_ast,
		parse::{
			Parser,
			error::{ParseError, ParseErrorKind},
		},
	},
	bytecode::{compile::PlanCompiler, explain::explain_bytecode},
	plan::{compile::core::plan as compile_plan, explain::explain_plans},
	token::{explain::explain_tokenize, span::Span, tokenize},
};
use reifydb_testing::{testscript, testscript::command::Command};
use reifydb_type::params::Params;
use test_each_file::test_each_path;

pub struct Runner {
	instance: Option<Database>,
	runtime: Option<Arc<Runtime>>,
}

impl testscript::runner::Runner for Runner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
		let mut output = String::new();
		match command.name.as_str() {
			"tokenize" => {
				let mut args = command.consume_args();
				let query = args.next_pos().ok_or("args not given")?.value.as_str();
				args.reject_rest()?;
				let result = explain_tokenize(query)?;
				writeln!(output, "{}", result)?;
			}
			"parse" => {
				let mut args = command.consume_args();
				let query = args.next_pos().ok_or("args not given")?.value.as_str();
				args.reject_rest()?;
				let result = explain_ast(query)?;
				writeln!(output, "{}", result)?;
			}
			"plan" => {
				let mut args = command.consume_args();
				let query = args.next_pos().ok_or("args not given")?.value.as_str();
				args.reject_rest()?;

				let engine = self.instance.as_ref().unwrap().engine().clone();
				let catalog = engine.catalog();
				let bump = Bump::new();

				// Tokenize and parse
				let tokens = tokenize(query, &bump).map_err(|e| {
					Box::new(ParseError {
						kind: ParseErrorKind::Custom(format!("Lex error: {}", e)),
						span: Span::default(),
					}) as Box<dyn Error>
				})?;
				let parser = Parser::new(&bump, tokens.tokens.into_bump_slice(), query);
				let parse_result = parser.parse();

				if !parse_result.errors.is_empty() {
					return Err(Box::new(parse_result.errors[0].clone()) as Box<dyn Error>);
				}

				// Compile to plan (synchronous, uses materialized catalog)
				let plans = compile_plan(&bump, &catalog.materialized, parse_result.program)
					.map_err(|e| format!("plan error: {}", e))?;
				let result = explain_plans(plans);

				write!(output, "{}", result)?;
			}
			"bytecode" => {
				let mut args = command.consume_args();
				let query = args.next_pos().ok_or("args not given")?.value.as_str();
				args.reject_rest()?;

				let engine = self.instance.as_ref().unwrap().engine().clone();
				let catalog = engine.catalog();
				let bump = Bump::new();

				// Tokenize and parse
				let tokens = tokenize(query, &bump).map_err(|e| {
					Box::new(ParseError {
						kind: ParseErrorKind::Custom(format!("Lex error: {}", e)),
						span: Span::default(),
					}) as Box<dyn Error>
				})?;
				let parser = Parser::new(&bump, tokens.tokens.into_bump_slice(), query);
				let parse_result = parser.parse();

				if !parse_result.errors.is_empty() {
					return Err(Box::new(parse_result.errors[0].clone()) as Box<dyn Error>);
				}

				// Compile to plan (synchronous, uses materialized catalog)
				let plans = compile_plan(&bump, &catalog.materialized, parse_result.program)
					.map_err(|e| format!("plan error: {}", e))?;

				// Compile plans to bytecode
				let program = PlanCompiler::compile(plans)
					.map_err(|e| format!("bytecode compile error: {}", e))?;

				let result = explain_bytecode(&program);

				write!(output, "{}", result)?;
			}
			_ => unimplemented!("unknown command: {}", command.name),
		}
		Ok(output)
	}

	fn start_script(&mut self) -> Result<(), Box<dyn Error>> {
		if let (Some(_runtime), Some(instance)) = (self.runtime.as_mut(), self.instance.as_mut()) {
			instance.start()?;
			instance.command_as_root(
				r#"
				create namespace test;
				create table test.users{id: int8, name: utf8, age: int4};
				create table test.orders{id: int8, user_id: int8, amount: float8};
			"#,
				Params::None,
			)?;
		}
		Ok(())
	}

	fn end_script(&mut self) -> Result<(), Box<dyn Error>> {
		if let (Some(_runtime), Some(instance)) = (self.runtime.as_mut(), self.instance.as_mut()) {
			instance.stop()?;
		}
		Ok(())
	}
}

impl Runner {
	pub fn new(runtime: Arc<Runtime>) -> Self {
		Self {
			instance: Some(embedded::memory().build().unwrap()),
			runtime: Some(runtime),
		}
	}
}

test_each_path! { in "crates/rqlv2/tests/scripts/token" as tokenize => run_test }
test_each_path! { in "crates/rqlv2/tests/scripts/ast" as ast => run_test }
test_each_path! { in "crates/rqlv2/tests/scripts/bytecode" as bytecode => run_plan_test }
test_each_path! { in "crates/rqlv2/tests/scripts/plan" as plan => run_plan_test }

fn run_test(path: &Path) {
	testscript::runner::run_path(
		&mut Runner {
			instance: None,
			runtime: None,
		},
		path,
	)
	.expect("test failed")
}

fn run_plan_test(path: &Path) {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	testscript::runner::run_path(&mut Runner::new(Arc::clone(&runtime)), path).expect("test failed")
}
