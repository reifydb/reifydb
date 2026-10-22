// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod cli;

use clap::Parser;
use reifydb::{WithSubsystem, allocator, server, system};
use reifydb_place::{migration_path, seed};

use crate::cli::RunArgs;

allocator::set_global_allocator!();

fn main() {
	allocator::verify();
	system::raise_fd_limit();
	let args = RunArgs::parse();
	let ws_bind = args.ws_bind.clone();
	let mut db = server::memory()
		.with_ws(move |ws| ws.bind_addr(ws_bind))
		.with_flow(|flow| flow)
		.with_migrations(migration_path())
		.build()
		.expect("failed to build reifydb database");
	seed(&db).expect("seed failed");
	db.start_and_await_signal().expect("database shutdown failed");
}
