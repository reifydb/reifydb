// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod catalog;
mod context;
mod dbstat;
mod report;

use std::{path::Path, process::exit};

use clap::{Parser, Subcommand};
use context::Context;
use reifydb::allocator;
use report::GroupBy;

allocator::set_global_allocator!();

type Result<T> = std::result::Result<T, String>;

#[derive(Parser)]
#[command(name = "reifydb-dev", about = "ReifyDB developer/debug tools", version)]
struct Cli {
	#[command(subcommand)]
	command: Command,
}

#[derive(Subcommand)]
enum Command {
	Storage(StorageArgs),
	Catalog(CatalogArgs),
}

#[derive(Parser)]
struct StorageArgs {
	dir: String,
	#[arg(long, value_enum)]
	group_by: Option<GroupBy>,
	#[arg(long)]
	all: bool,
	#[arg(long, default_value_t = 40)]
	top: usize,
	#[arg(long)]
	filter: Option<String>,
	#[arg(long)]
	exact_rows: bool,
	#[arg(long)]
	no_rows: bool,
	#[arg(long)]
	json: bool,
}

#[derive(Parser)]
struct CatalogArgs {
	dir: String,
	#[arg(long)]
	json: bool,
}

fn main() {
	allocator::verify();
	let cli = Cli::parse();
	let ctx = Context::new();
	let result = match cli.command {
		Command::Storage(args) => storage(&ctx, args),
		Command::Catalog(args) => catalog_dump(args),
	};
	if let Err(e) = result {
		eprintln!("error: {e}");
		eprintln!(
			"hint: if catalog queries failed or names look wrong, build reifydb-dev from the \
			 same reifydb commit that produced the snapshot (the catalog decoders are version-coupled)."
		);
		exit(1);
	}
}

fn storage(ctx: &Context, args: StorageArgs) -> Result<()> {
	let multi_db = require_multi_db(&args.dir)?;
	let started = ctx.clock.instant();

	let phys = dbstat::read(&multi_db, args.exact_rows && !args.no_rows)?;

	eprintln!("opening {} via the embedded engine (this writes to the directory - use a copy)", args.dir);
	let cat = catalog::with_open(&args.dir, catalog::load)?;

	report::render(
		&cat,
		&phys,
		report::Options {
			group_by: args.group_by,
			all: args.all,
			top: args.top,
			filter: args.filter,
			json: args.json,
			show_rows: !args.no_rows,
		},
	);
	eprintln!("done in {:.1}s", started.elapsed().as_secs_f64());
	Ok(())
}

fn catalog_dump(args: CatalogArgs) -> Result<()> {
	eprintln!("opening {} via the embedded engine (this writes to the directory - use a copy)", args.dir);
	let cat = catalog::with_open(&args.dir, catalog::load)?;
	report::dump_catalog(&cat, args.json);
	Ok(())
}

fn require_multi_db(dir: &str) -> Result<String> {
	let path = Path::new(dir).join("multi.db");
	if !path.exists() {
		return Err(format!("no multi.db in '{dir}' (expected a sqlite database directory)"));
	}
	Ok(path.to_string_lossy().into_owned())
}
