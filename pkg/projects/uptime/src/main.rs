// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]

mod assets;
mod auth;
mod checks;
mod cli;
mod dto;
mod error;
mod routes;
mod scheduler;
mod schema;
mod state;
mod store;

use clap::Parser;
use reifydb::{SqliteConfig, WithSubsystem, allocator, server, system};
use tracing::info;

use crate::{cli::RunArgs, state::AppState};

allocator::set_global_allocator!();

fn main() {
	allocator::verify();
	system::raise_fd_limit();
	rustls::crypto::ring::default_provider()
		.install_default()
		.expect("failed to install rustls ring crypto provider");

	let args = RunArgs::parse();

	let builder = if args.memory {
		server::memory()
	} else {
		std::fs::create_dir_all(&args.data_dir).expect("failed to create data directory");
		server::sqlite(SqliteConfig::new(&args.data_dir))
	};

	let reifydb_http_bind = args.reifydb_http_bind.clone();
	let reifydb_ws_bind = args.reifydb_ws_bind.clone();
	let mut db = builder
		.with_http(move |http| http.bind_addr(reifydb_http_bind))
		.with_ws(move |ws| ws.bind_addr(reifydb_ws_bind))
		.with_migrations(schema::migrations())
		.with_tracing(|t| {
			t.with_console(|console| console.color(true)).with_filter("info,reifydb_uptime=debug")
		})
		.build()
		.expect("failed to build reifydb database");

	let state = AppState::new(&db, args);
	let handle = state.tokio.clone();

	let listener = handle
		.block_on(tokio::net::TcpListener::bind(&state.cfg.http_bind))
		.expect("failed to bind uptime http listener");
	info!("uptime server listening on {}", listener.local_addr().expect("listener has no local addr"));

	let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
	let server_task = handle.spawn(routes::serve(state.clone(), listener, shutdown_rx.clone()));
	let scheduler_task = handle.spawn(scheduler::run(state, shutdown_rx));

	let shutdown_handle = handle.clone();
	db.start_and_await_signal_with_shutdown(move || {
		let _ = shutdown_tx.send(true);
		shutdown_handle.block_on(async {
			let _ = server_task.await;
			let _ = scheduler_task.await;
		});
		Ok(())
	})
	.expect("database shutdown failed");
}
