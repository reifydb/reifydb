// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod catalog;
mod cdc;
mod context;
mod report;

use std::{fs::metadata, path::Path, process::exit};

use clap::{Parser, Subcommand};
use context::Context;
use reifydb::allocator;

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
	Catalog(CatalogArgs),
	Cdc(CdcArgs),
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

fn cdc_report(ctx: &Context, args: CdcArgs) -> Result<()> {
	let started = ctx.clock.instant();
	let stats = cdc::scan(&args.dir, !args.no_blocks)?;
	let file_bytes = metadata(Path::new(&args.dir).join("cdc.db")).map(|m| m.len()).unwrap_or(0);

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
