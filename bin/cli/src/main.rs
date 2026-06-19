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
	/// WebSocket protocol operations
	Ws(WsCommand),
	/// Local (embedded) database operations
	Local(LocalCommand),
	// Future: Http(HttpCommand), Grpc(GrpcCommand)
}

#[derive(Parser)]
struct LocalCommand {
	#[command(subcommand)]
	action: LocalAction,
}

#[derive(Subcommand)]
enum LocalAction {
	/// Export shapes from a local database to a self-contained .rql script
	Export(LocalExportArgs),
	/// Import a .rql script into a local database
	Import(LocalImportArgs),
}

#[derive(Parser)]
struct LocalExportArgs {
	/// Path to the local (sqlite) database
	#[arg(long)]
	db: String,

	/// Output file (defaults to stdout)
	#[arg(long, short)]
	out: Option<String>,

	/// Export only this namespace (repeatable)
	#[arg(long)]
	namespace: Vec<String>,

	/// Export only this shape, given as namespace::name (repeatable)
	#[arg(long)]
	shape: Vec<String>,

	/// Export only this kind: table|ringbuffer|series|dictionary|enum (repeatable)
	#[arg(long)]
	kind: Vec<String>,

	/// Export schema (DDL) only, without data
	#[arg(long)]
	schema_only: bool,

	/// Export data only, assuming the schema already exists in the target
	#[arg(long)]
	data_only: bool,

	/// Rows per INSERT batch
	#[arg(long)]
	batch_size: Option<usize>,

	/// Emit IF NOT EXISTS on the CREATE statements that support it
	#[arg(long)]
	if_not_exists: bool,
}

#[derive(Parser)]
struct LocalImportArgs {
	/// Path to the local (sqlite) database
	#[arg(long)]
	db: String,

	/// Path to the .rql script to import
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
	/// Execute query statements (SELECT, FROM, etc.)
	Query(QueryArgs),
	/// Execute admin statements (DDL + DML + Query)
	Admin(AdminArgs),
	/// Execute command statements (INSERT, UPDATE, DELETE)
	Command(CommandArgs),
	/// Start interactive REPL session
	Repl(ReplArgs),
}

#[derive(Parser)]
struct QueryArgs {
	/// Server host
	#[arg(long, env = "REIFYDB_HOST", default_value = "127.0.0.1")]
	host: String,

	/// Server port
	#[arg(long, env = "REIFYDB_PORT", default_value = "8090")]
	port: u16,

	/// Authentication token
	#[arg(long, short, env = "REIFYDB_TOKEN")]
	token: Option<String>,

	/// Statement(s) to execute (optional -c flag for quoting)
	#[arg(short, long)]
	c: Option<String>,

	/// Statement(s) as trailing arguments (no quotes needed)
	/// Example: reifydb ws query FROM system.tables
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
	/// Server host
	#[arg(long, env = "REIFYDB_HOST", default_value = "127.0.0.1")]
	host: String,

	/// Server port
	#[arg(long, env = "REIFYDB_PORT", default_value = "8090")]
	port: u16,

	/// Authentication token
	#[arg(long, short, env = "REIFYDB_TOKEN")]
	token: Option<String>,

	/// Statement(s) to execute (optional -c flag for quoting)
	#[arg(short, long)]
	c: Option<String>,

	/// Statement(s) as trailing arguments (no quotes needed)
	/// Example: reifydb ws command CREATE TABLE users { id: int4 }
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
	/// Server host
	#[arg(long, env = "REIFYDB_HOST", default_value = "127.0.0.1")]
	host: String,

	/// Server port
	#[arg(long, env = "REIFYDB_PORT", default_value = "8090")]
	port: u16,

	/// Authentication token
	#[arg(long, short, env = "REIFYDB_TOKEN")]
	token: Option<String>,

	/// Statement(s) to execute (optional -c flag for quoting)
	#[arg(short, long)]
	c: Option<String>,

	/// Statement(s) as trailing arguments (no quotes needed)
	/// Example: reifydb ws admin CREATE TABLE users { id: int4 }
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
	/// Server host
	#[arg(long, env = "REIFYDB_HOST", default_value = "127.0.0.1")]
	host: String,

	/// Server port
	#[arg(long, env = "REIFYDB_PORT", default_value = "8090")]
	port: u16,

	/// Authentication token
	#[arg(long, short, env = "REIFYDB_TOKEN")]
	token: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
	allocator::verify();

	let cli = Cli::parse();

	match cli.protocol {
		Protocol::Ws(ws_cmd) => handle_ws(ws_cmd).await,
		Protocol::Local(local_cmd) => {
			// Run on a dedicated thread so the embedded database gets a clean
			// execution context outside the tokio runtime driving the CLI.
			match thread::spawn(move || local::handle(local_cmd)).join() {
				Ok(result) => result.map_err(|e| e.into()),
				Err(_) => Err("local command thread panicked".into()),
			}
		}
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
