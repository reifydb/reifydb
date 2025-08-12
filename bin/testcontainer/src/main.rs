// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb::WithHooks;
use reifydb::server;
use std::thread;
use tokio::runtime::Runtime;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::oneshot;
use tokio::{select, signal};
use reifydb::network::ws::server::WsConfig;

fn main() {
    let (tx, rx) = oneshot::channel();

    thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        let _ = rt.block_on(async move {
            let mut sigterm = signal(SignalKind::terminate()).unwrap();

            let tokio_signal = async {
                signal::ctrl_c().await.expect("Failed to listen for shutdown signal");
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
        .with_ws(WsConfig { socket: "0.0.0.0:8090".parse().ok() })
        .on_create(|ctx| {
            ctx.command_as_root("create schema test", ())?;
            ctx.command_as_root("create table test.arith { id: int1, value: int2, num: int2 }", ())?;
            ctx.command_as_root(
                "from [
                { id: 1, value: 1, num: 5  },
                { id: 1, value: 1, num: 10 },
                { id: 1, value: 2, num: 15 },
                { id: 2, value: 1, num: 10 },
                { id: 2, value: 1, num: 30 }
              ] insert test.arith
            ",
                (),
            )?;
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
