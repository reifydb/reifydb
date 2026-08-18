// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod catalog;
mod cdc;
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
	Cdc(CdcArgs),
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

#[derive(Parser)]
struct CdcArgs {
	dir: String,
	#[arg(long)]
	all: bool,
	#[arg(long, default_value_t = 40)]
	top: usize,
	#[arg(long)]
	no_names: bool,
	#[arg(long)]
	no_blocks: bool,
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
		Command::Cdc(args) => cdc_report(&ctx, args),
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

fn cdc_report(ctx: &Context, args: CdcArgs) -> Result<()> {
	let started = ctx.clock.instant();
	let stats = cdc::scan(&args.dir, !args.no_blocks)?;
	let file_bytes = std::fs::metadata(Path::new(&args.dir).join("cdc.db")).map(|m| m.len()).unwrap_or(0);

	let cat = if args.no_names {
		None
	} else {
		eprintln!("opening {} via the embedded engine (this writes to the directory - use a copy)", args.dir);
		Some(catalog::with_open(&args.dir, catalog::load)?)
	};

	report::render_cdc(
		cat.as_ref(),
		&stats,
		file_bytes,
		report::CdcOptions {
			all: args.all,
			top: args.top,
			json: args.json,
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
