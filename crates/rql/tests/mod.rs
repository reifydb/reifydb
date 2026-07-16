// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{error::Error, fmt::Write, path::Path, str::FromStr, sync::Arc};

use reifydb::{Database, Params, RuntimeConfig, embedded as db_embedded};
use reifydb_core::{common::CommitVersion, interface::catalog::config::ConfigKey};
use reifydb_testing::{testscript, testscript::command::Command};
use reifydb_value::value::{Value, value_type::ValueType};
use test_each_file::test_each_path;
use tokio::runtime::Runtime;

pub struct Runner {
	instance: Database,
	next_config_version: u64,
}

impl Runner {
	pub fn new() -> Self {
		Self {
			instance: db_embedded::memory()
				.with_runtime_config(RuntimeConfig::default().seeded(0))
				.build()
				.unwrap(),
			next_config_version: 1,
		}
	}
}

impl testscript::runner::Runner for Runner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
		let mut output = String::new();
		match command.name.as_str() {
			"admin" => {
				let rql = command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");
				for frame in self.instance.admin_as_root(rql.as_str(), Params::None)? {
					writeln!(output, "{}", frame).unwrap();
				}
			}
			"command" => {
				let rql = command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");
				for frame in self.instance.command_as_root(rql.as_str(), Params::None)? {
					writeln!(output, "{}", frame).unwrap();
				}
			}
			"query" => {
				let rql = command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");
				for frame in self.instance.query_as_root(rql.as_str(), Params::None)? {
					writeln!(output, "{}", frame).unwrap();
				}
			}
			"query_fails" => {
				let rql = command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");
				match self.instance.query_as_root(rql.as_str(), Params::None) {
					Ok(_) => return Err("expected query to fail, but it succeeded".into()),
					Err(e) => writeln!(output, "Error {}", e.0.code).unwrap(),
				}
			}
			"set_config" => {
				let key_arg = command.args.first().ok_or("set_config requires a KEY argument")?;
				let value_arg = command.args.get(1).ok_or("set_config requires a VALUE argument")?;
				let key = ConfigKey::from_str(key_arg.value.as_str())?;
				let value = match key.expected_types().first() {
					Some(ValueType::Uint8) => Value::Uint8(value_arg.value.parse::<u64>()?),
					Some(ValueType::Uint4) => Value::Uint4(value_arg.value.parse::<u32>()?),
					Some(ValueType::Uint2) => Value::Uint2(value_arg.value.parse::<u16>()?),
					Some(ValueType::Uint1) => Value::Uint1(value_arg.value.parse::<u8>()?),
					other => {
						return Err(format!(
							"set_config: unsupported config value type {other:?}"
						)
						.into());
					}
				};
				self.instance.catalog().cache().set_config(
					key,
					CommitVersion(self.next_config_version),
					value,
				)?;
				self.next_config_version += 1;
				writeln!(output, "ok").unwrap();
			}
			name => {
				return Err(format!("invalid command {name}").into());
			}
		}
		Ok(output)
	}

	fn start_script(&mut self) -> Result<(), Box<dyn Error>> {
		self.instance.admin_as_root(
			"create table users { id: int4, name: utf8, age: int4, email: utf8, status: utf8, active: bool, amount: int4, price: int4, created_at: datetime }",
			Params::None,
		)?;
		self.instance.admin_as_root("create table orders { id: int4, user_id: int4 }", Params::None)?;
		self.instance.admin_as_root("create namespace test", Params::None)?;
		self.instance.admin_as_root(
			"create table test::users { id: int4, name: utf8, age: int4, email: utf8, status: utf8, active: bool, created_at: datetime }",
			Params::None,
		)?;
		Ok(())
	}

	fn end_script(&mut self) -> Result<(), Box<dyn Error>> {
		self.instance.stop()?;
		Ok(())
	}
}

test_each_path! { in "crates/rql/tests/scripts/tokenize" as tokenize => test_embedded }
test_each_path! { in "crates/rql/tests/scripts/ast" as ast => test_embedded }
test_each_path! { in "crates/rql/tests/scripts/logical" as logical => test_embedded }
test_each_path! { in "crates/rql/tests/scripts/explain" as explain => test_embedded }
test_each_path! { in "crates/rql/tests/scripts/memory_limit" as memory_limit => test_embedded }

fn test_embedded(path: &Path) {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	testscript::runner::run_path(&mut Runner::new(), path).expect("test failed")
}
