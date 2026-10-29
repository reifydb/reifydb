// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod catalog;
mod report;

use std::process::exit;

use clap::{Parser, Subcommand};
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
	let result = match cli.command {
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

fn catalog_dump(args: CatalogArgs) -> Result<()> {
	eprintln!("opening {} via the embedded engine (this writes to the directory - use a copy)", args.dir);
	let cat = catalog::with_open(&args.dir, catalog::load)?;
	report::dump_catalog(&cat, args.json);
	Ok(())
}
