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
mod probe;
mod routes;
mod scheduler;
mod schema;
mod state;
mod store;

use std::{
	fs::create_dir_all,
	sync::atomic::{AtomicBool, Ordering},
};

use clap::Parser;
use libc::{SIGHUP, SIGINT, SIGQUIT, SIGTERM, c_int, sighandler_t, signal};
use reifydb::{
	Clock, SqliteConfig, WithSubsystem, allocator, runtime::context::rng::Rng, server, system,
	value::value::duration::Duration,
};
use reifydb_client::{WireFormat, WsClient};
use rustls::crypto::ring::default_provider;
use tokio::{net::TcpListener, sync::watch, time::interval};
use tracing::info;

use crate::{checks::CheckContext, cli::RunArgs, state::AppState, store::ProbeBackend};

allocator::set_global_allocator!();

const PROBES: [(&str, &str); 2] = [("probe-a", "US East"), ("probe-b", "EU West")];

fn region_for_probe(name: &str) -> Option<&'static str> {
	PROBES.iter().find(|(n, _)| *n == name).map(|(_, region)| *region)
}

static PROBE_SHUTDOWN: AtomicBool = AtomicBool::new(false);

extern "C" fn handle_probe_signal(_sig: c_int) {
	PROBE_SHUTDOWN.store(true, Ordering::SeqCst);
}

fn install_probe_signal_handlers() {
	// SAFETY: `signal` sets process-wide signal dispositions; this is sound because
	// `handle_probe_signal` is async-signal-safe - its whole body is a single atomic
	// store, the only class of operation permitted inside a C signal handler.
	unsafe {
		signal(SIGINT, handle_probe_signal as sighandler_t);
		signal(SIGTERM, handle_probe_signal as sighandler_t);
		signal(SIGQUIT, handle_probe_signal as sighandler_t);
		signal(SIGHUP, handle_probe_signal as sighandler_t);
	}
}

async fn await_termination_signal() {
	#[allow(clippy::disallowed_types)]
	let mut poll = interval(Duration::from_milliseconds(200).unwrap().to_std());
	loop {
		poll.tick().await;
		if PROBE_SHUTDOWN.load(Ordering::SeqCst) {
			return;
		}
	}
}

fn main() {
	allocator::verify();
	system::raise_fd_limit();
	default_provider().install_default().expect("failed to install rustls ring crypto provider");

	let args = RunArgs::parse();

	if let Some(token) = args.probe_token.clone() {
		run_standalone_probe(args, token);
		return;
	}

	let builder = if args.memory {
		server::memory()
	} else {
		create_dir_all(&args.data_dir).expect("failed to create data directory");
		server::sqlite(SqliteConfig::new(&args.data_dir))
	};

	let reifydb_http_bind = args.reifydb_http_bind.clone();
	let reifydb_ws_bind = args.reifydb_ws_bind.clone();
	let mut db = builder
		.with_http(move |http| http.bind_addr(reifydb_http_bind))
		.with_ws(move |ws| ws.bind_addr(reifydb_ws_bind))
		.with_flow(|flow| flow)
		.with_migrations(schema::migrations())
		.with_tracing(|t| {
			t.with_console(|console| console.color(true)).with_filter("info,reifydb_uptime=debug")
		})
		.build()
		.expect("failed to build reifydb database");

	let state = AppState::new(&db, args);
	let handle = state.tokio.clone();

	let listener =
		handle.block_on(TcpListener::bind(&state.cfg.http_bind)).expect("failed to bind uptime http listener");
	info!("uptime server listening on {}", listener.local_addr().expect("listener has no local addr"));

	let (shutdown_tx, shutdown_rx) = watch::channel(false);
	let server_task = handle.spawn(routes::serve(state.clone(), listener, shutdown_rx.clone()));

	let mut probe_tasks = Vec::new();
	for (name, region_label) in PROBES {
		let id = handle
			.block_on(store::find_identity_by_name(&state, name))
			.unwrap_or_else(|e| panic!("failed to look up probe {name}: {e:?}"))
			.unwrap_or_else(|| panic!("probe identity {name} missing - migrations must provision it"));
		if state.cfg.no_embedded_probes {
			info!("probe {name} provisioned as service identity {id} (embedded probe disabled)");
			continue;
		}
		let backend = ProbeBackend::Embedded {
			engine: state.engine.clone(),
			rng: state.rng.clone(),
			tokio: handle.clone(),
			identity: id,
		};
		let region_id = handle
			.block_on(store::region_id_by_label(&backend, region_label))
			.unwrap_or_else(|e| panic!("failed to resolve region for probe {name}: {e:?}"))
			.unwrap_or_else(|| panic!("region {region_label:?} not found for probe {name}"));
		handle.block_on(store::register_probe(&backend, id, name, state.clock.now()))
			.expect("failed to register probe");
		info!("probe {name} running as service identity {id} serving region {region_label}");
		probe_tasks.push(handle.spawn(probe::run(
			backend,
			state.check_context(),
			id,
			name.to_string(),
			region_id,
			shutdown_rx.clone(),
		)));
	}

	let scheduler_task = handle.spawn(scheduler::run(state, shutdown_rx));

	let shutdown_handle = handle.clone();
	db.start_and_await_signal_with_shutdown(move || {
		let _ = shutdown_tx.send(true);
		shutdown_handle.block_on(async {
			let _ = server_task.await;
			let _ = scheduler_task.await;
			for task in probe_tasks {
				let _ = task.await;
			}
		});
		Ok(())
	})
	.expect("database shutdown failed");
}

fn run_standalone_probe(args: RunArgs, token: String) {
	let ws = args.probe_reifydb_ws.clone().expect("UPTIME_PROBE_REIFYDB_WS must be set for a standalone probe");

	let runtime = tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.build()
		.expect("failed to build tokio runtime");

	runtime.block_on(async move {
		let mut client = WsClient::connect(&ws, WireFormat::Frames)
			.await
			.expect("failed to connect to reifydb ws endpoint");
		let login = client.login_with_token(&token).await.expect("probe token login failed");
		info!("probe authenticated as identity {}", login.identity);

		let backend = ProbeBackend::Remote {
			client,
		};
		let (id, name) = store::probe_self(&backend).await.expect("failed to resolve probe identity");
		let region_label = region_for_probe(&name).unwrap_or_else(|| panic!("no region mapping for probe {name}"));
		let region_id = store::region_id_by_label(&backend, region_label)
			.await
			.unwrap_or_else(|e| panic!("failed to resolve region for probe {name}: {e:?}"))
			.unwrap_or_else(|| panic!("region {region_label:?} not found for probe {name}"));

		let ctx = CheckContext {
			clock: Clock::Real,
			rng: Rng::Os,
			http: reqwest::Client::builder()
				.redirect(reqwest::redirect::Policy::limited(5))
				.build()
				.expect("failed to build http client"),
			allow_private_targets: args.allow_private_targets,
		};

		store::register_probe(&backend, id, &name, ctx.clock.now()).await.expect("failed to register probe");
		info!("probe {name} running (standalone) as service identity {id} serving region {region_label}");

		install_probe_signal_handlers();
		let (shutdown_tx, shutdown_rx) = watch::channel(false);
		let probe_fut = probe::run(backend, ctx, id, name, region_id, shutdown_rx);
		tokio::pin!(probe_fut);

		tokio::select! {
			_ = &mut probe_fut => {}
			_ = await_termination_signal() => {
				info!("termination signal received, draining in-flight check and shutting down");
				let _ = shutdown_tx.send(true);
				probe_fut.await;
			}
		}
	});
}

#[cfg(test)]
mod tests {
	use std::sync::atomic::Ordering;

	use super::{PROBE_SHUTDOWN, await_termination_signal, install_probe_signal_handlers};

	#[tokio::test]
	async fn termination_signal_flips_the_flag_and_wakes_the_waiter() {
		// The standalone probe's graceful stop hinges on a real OS signal flipping the
		// flag the poll loop watches. Install the handlers, raise SIGTERM at ourselves -
		// the handler only stores an atomic, so it does not terminate the test - and
		// require await_termination_signal to observe it and return. If the handler is
		// misinstalled or the poll never checks the flag, this hangs and the timeout fails.
		assert!(!PROBE_SHUTDOWN.load(Ordering::SeqCst), "flag must start clear");
		install_probe_signal_handlers();

		// SAFETY: raising a signal whose handler we just installed; that handler is
		// async-signal-safe and does not terminate the process.
		unsafe {
			libc::raise(libc::SIGTERM);
		}

		#[allow(clippy::disallowed_types)]
		let wait = tokio::time::timeout(std::time::Duration::from_secs(2), await_termination_signal());
		wait.await.expect("await_termination_signal must return after SIGTERM");
		assert!(PROBE_SHUTDOWN.load(Ordering::SeqCst), "SIGTERM must set the shutdown flag");
	}
}
