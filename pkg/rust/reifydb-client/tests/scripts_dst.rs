// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg(reifydb_single_threaded)]

mod common;

use std::{error::Error, path::Path};

use common::{
	DstTestContext, dst_response_to_result, parse_named_params, parse_positional_params, parse_rql, write_frames,
};
use reifydb_client::Params;
use reifydb_testing::{testscript, testscript::command::Command};
use test_each_file::test_each_path;

pub struct DstRunner {
	ctx: Option<DstTestContext>,
}

impl DstRunner {
	pub fn new() -> Self {
		Self {
			ctx: None,
		}
	}
}

impl testscript::runner::Runner for DstRunner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
		let ctx = self.ctx.as_ref().ok_or("No DST context available")?;
		let identity = ctx.identity;

		match command.name.as_str() {
			"admin" => {
				let rql = parse_rql(command);
				println!("admin: {rql}");
				let response = ctx.client.admin(identity, vec![rql], Params::None);
				write_frames(dst_response_to_result(response)?)
			}

			"command" => {
				let rql = parse_rql(command);
				println!("command: {rql}");
				let response = ctx.client.command(identity, vec![rql], Params::None);
				write_frames(dst_response_to_result(response)?)
			}

			"command_positional" => {
				let (rql, params) = parse_positional_params(command);
				println!("command_positional: {rql}");
				let response = ctx.client.command(identity, vec![rql], params);
				write_frames(dst_response_to_result(response)?)
			}

			"command_named" => {
				let (rql, params) = parse_named_params(command);
				println!("command_named: {rql}");
				let response = ctx.client.command(identity, vec![rql], params);
				write_frames(dst_response_to_result(response)?)
			}

			"query" => {
				let rql = parse_rql(command);
				println!("query: {rql}");
				let response = ctx.client.query(identity, vec![rql], Params::None);
				write_frames(dst_response_to_result(response)?)
			}

			"query_positional" => {
				let (rql, params) = parse_positional_params(command);
				println!("query_positional: {rql}");
				let response = ctx.client.query(identity, vec![rql], params);
				write_frames(dst_response_to_result(response)?)
			}

			"query_named" => {
				let (rql, params) = parse_named_params(command);
				println!("query_named: {rql}");
				let response = ctx.client.query(identity, vec![rql], params);
				write_frames(dst_response_to_result(response)?)
			}

			name => Err(format!("invalid command {name}").into()),
		}
	}

	fn start_script(&mut self) -> Result<(), Box<dyn Error>> {
		self.ctx = Some(DstTestContext::new());
		Ok(())
	}

	fn end_script(&mut self) -> Result<(), Box<dyn Error>> {
		self.ctx = None;
		Ok(())
	}
}

test_each_path! { in "pkg/rust/reifydb-client/tests/scripts" as scripts_dst => test_dst }

fn test_dst(path: &Path) {
	testscript::runner::run_path(&mut DstRunner::new(), path).unwrap()
}
