// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod catalog;
mod report;

use std::{
	io::{read_to_string, stdin},
	process::exit,
};

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
	Query(QueryArgs),
}

#[derive(Parser)]
struct CatalogArgs {
	dir: String,
	#[arg(long)]
	json: bool,
	#[arg(long)]
	wal_off: bool,
}

#[derive(Parser)]
struct QueryArgs {
	dir: String,
	#[arg(long)]
	wal_off: bool,
}

fn main() {
	allocator::verify();
	let cli = Cli::parse();
	let result = match cli.command {
		Command::Catalog(args) => catalog_dump(args),
		Command::Query(args) => query(args),
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
	let cat = catalog::with_open(&args.dir, args.wal_off, catalog::load)?;
	report::dump_catalog(&cat, args.json);
	Ok(())
}

fn query(args: QueryArgs) -> Result<()> {
	let input = read_to_string(stdin()).map_err(|e| format!("failed to read stdin: {e}"))?;
	eprintln!("opening {} via the embedded engine (this writes to the directory - use a copy)", args.dir);
	catalog::with_open(&args.dir, args.wal_off, |db| {
		for rql in input.lines().map(str::trim).filter(|line| !line.is_empty()) {
			println!("## {rql}");
			let rows = catalog::rows(db, rql)?;
			if let Some(first) = rows.first() {
				println!(
					"{}",
					first.iter().map(|(name, _)| name.as_str()).collect::<Vec<_>>().join("\t")
				);
			}
			for row in &rows {
				println!(
					"{}",
					row.iter().map(|(_, value)| value.to_string()).collect::<Vec<_>>().join("\t")
				);
			}
		}
		Ok(())
	})
}
