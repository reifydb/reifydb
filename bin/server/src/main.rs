// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::thread;

use reifydb::{WithHooks, network::ws::server::WsConfig, server};
use tokio::{
	runtime::Runtime,
	select, signal,
	signal::unix::{SignalKind, signal},
	sync::oneshot,
};

fn main() {
	let (tx, rx) = oneshot::channel();

	thread::spawn(move || {
		let rt = Runtime::new().unwrap();
		let _ = rt.block_on(async move {
			let mut sigterm =
				signal(SignalKind::terminate()).unwrap();

			let tokio_signal = async {
				signal::ctrl_c().await.expect(
					"Failed to listen for shutdown signal",
				);
				println!("Shutdown signal received");
			};

			select! {
			    _ = tokio_signal => {
				println!("Shutting down...");
				tx.send(()).unwrap();
			    }
			    _ = sigterm.recv() => {
				println!("Received SIGTERM. Cleaning up resources...");
				tx.send(()).unwrap();
			    }
			}
		});
	});

	let rt = Runtime::new().unwrap();
	let mut db = server::memory_serializable()
		.with_ws(WsConfig {
			socket: "0.0.0.0:8090".parse().ok(),
		})
		.on_create(|ctx| {
			ctx.command_as_root("create schema test", ())?;
			Ok(())
		})
		.build();

	// Start the database
	db.start().unwrap();

	// Wait for shutdown signal
	rt.block_on(async {
		rx.await.unwrap();
	});

	// Stop the database
	db.stop().unwrap();
}
