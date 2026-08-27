// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]

mod local;
mod ws;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
use std::thread;

use clap::{Parser, Subcommand};
use reifydb::allocator;

allocator::set_global_allocator!();

#[derive(Parser)]
#[command(name = "reifydb")]
#[command(about = "ReifyDB CLI - A powerful database client", long_about = None)]
#[command(version)]
struct Cli {
	#[command(subcommand)]
	protocol: Protocol,
}

#[derive(Subcommand)]
enum Protocol {
	Ws(WsCommand),
	Local(LocalCommand),
}

#[derive(Parser)]
struct LocalCommand {
	#[command(subcommand)]
	action: LocalAction,
}

#[derive(Subcommand)]
enum LocalAction {
	Export(LocalExportArgs),
	Import(LocalImportArgs),
}

#[derive(Parser)]
struct LocalExportArgs {
	#[arg(long)]
	db: String,

	#[arg(long, short)]
	out: Option<String>,

	#[arg(long)]
	namespace: Vec<String>,

	#[arg(long)]
	object: Vec<String>,

	#[arg(long)]
	kind: Vec<String>,

	#[arg(long)]
	schema_only: bool,

	#[arg(long)]
	data_only: bool,

	#[arg(long)]
	batch_size: Option<usize>,

	#[arg(long)]
	if_not_exists: bool,
}

#[derive(Parser)]
struct LocalImportArgs {
	#[arg(long)]
	db: String,

	#[arg(long, short)]
	file: String,
}

#[derive(Parser)]
struct WsCommand {
	#[command(subcommand)]
	action: WsAction,
}

#[derive(Subcommand)]
enum WsAction {
	Query(QueryArgs),
	Admin(AdminArgs),
	Command(CommandArgs),
	Repl(ReplArgs),
}

#[derive(Parser)]
struct QueryArgs {
	#[arg(long, env = "REIFYDB_HOST", default_value = "127.0.0.1")]
	host: String,

	#[arg(long, env = "REIFYDB_PORT", default_value = "8090")]
	port: u16,

	#[arg(long, short, env = "REIFYDB_TOKEN")]
	token: Option<String>,

	#[arg(short, long)]
	c: Option<String>,

	#[arg(trailing_var_arg = true, allow_hyphen_values = true)]
	rql: Vec<String>,
}

impl QueryArgs {
	fn get_rql(&self) -> String {
		if let Some(ref stmt) = self.c {
			stmt.clone()
		} else {
			self.rql.join(" ")
		}
	}
}

#[derive(Parser)]
struct CommandArgs {
	#[arg(long, env = "REIFYDB_HOST", default_value = "127.0.0.1")]
	host: String,

	#[arg(long, env = "REIFYDB_PORT", default_value = "8090")]
	port: u16,

	#[arg(long, short, env = "REIFYDB_TOKEN")]
	token: Option<String>,

	#[arg(short, long)]
	c: Option<String>,

	#[arg(trailing_var_arg = true, allow_hyphen_values = true)]
	rql: Vec<String>,
}

impl CommandArgs {
	fn get_rql(&self) -> String {
		if let Some(ref stmt) = self.c {
			stmt.clone()
		} else {
			self.rql.join(" ")
		}
	}
}

#[derive(Parser)]
struct AdminArgs {
	#[arg(long, env = "REIFYDB_HOST", default_value = "127.0.0.1")]
	host: String,

	#[arg(long, env = "REIFYDB_PORT", default_value = "8090")]
	port: u16,

	#[arg(long, short, env = "REIFYDB_TOKEN")]
	token: Option<String>,

	#[arg(short, long)]
	c: Option<String>,

	#[arg(trailing_var_arg = true, allow_hyphen_values = true)]
	rql: Vec<String>,
}

impl AdminArgs {
	fn get_rql(&self) -> String {
		if let Some(ref stmt) = self.c {
			stmt.clone()
		} else {
			self.rql.join(" ")
		}
	}
}

#[derive(Parser)]
struct ReplArgs {
	#[arg(long, env = "REIFYDB_HOST", default_value = "127.0.0.1")]
	host: String,

	#[arg(long, env = "REIFYDB_PORT", default_value = "8090")]
	port: u16,

	#[arg(long, short, env = "REIFYDB_TOKEN")]
	token: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
	allocator::verify();

	let cli = Cli::parse();

	match cli.protocol {
		Protocol::Ws(ws_cmd) => handle_ws(ws_cmd).await,
		Protocol::Local(local_cmd) => match thread::spawn(move || local::handle(local_cmd)).join() {
			Ok(result) => result.map_err(|e| e.into()),
			Err(_) => Err("local command thread panicked".into()),
		},
	}
}

async fn handle_ws(ws_cmd: WsCommand) -> Result<()> {
	match ws_cmd.action {
		WsAction::Query(args) => {
			let rql = args.get_rql();
			ws::query::execute_query(&args.host, args.port, args.token, &rql).await
		}
		WsAction::Admin(args) => {
			let rql = args.get_rql();
			ws::admin::execute_admin(&args.host, args.port, args.token, &rql).await
		}
		WsAction::Command(args) => {
			let rql = args.get_rql();
			ws::command::execute_command(&args.host, args.port, args.token, &rql).await
		}
		WsAction::Repl(args) => ws::repl::start_repl(&args.host, args.port, args.token).await,
	}
}
