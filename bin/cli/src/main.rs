// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod ws;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
use clap::{Parser, Subcommand};

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
	// Future: Http(HttpCommand), Grpc(GrpcCommand), Local(LocalCommand)
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
	/// Execute command statements (CREATE, INSERT, UPDATE, etc.)
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
	statement: Vec<String>,
}

impl QueryArgs {
	fn get_statements(&self) -> String {
		if let Some(ref stmt) = self.c {
			stmt.clone()
		} else {
			self.statement.join(" ")
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
	statement: Vec<String>,
}

impl CommandArgs {
	fn get_statements(&self) -> String {
		if let Some(ref stmt) = self.c {
			stmt.clone()
		} else {
			self.statement.join(" ")
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

fn main() -> Result<()> {
	let cli = Cli::parse();

	match cli.protocol {
		Protocol::Ws(ws_cmd) => handle_ws(ws_cmd),
	}
}

fn handle_ws(ws_cmd: WsCommand) -> Result<()> {
	match ws_cmd.action {
		WsAction::Query(args) => {
			let statements = args.get_statements();
			ws::query::execute_query(&args.host, args.port, args.token, &statements)
		}
		WsAction::Command(args) => {
			let statements = args.get_statements();
			ws::command::execute_command(&args.host, args.port, args.token, &statements)
		}
		WsAction::Repl(args) => ws::repl::start_repl(&args.host, args.port, args.token),
	}
}
