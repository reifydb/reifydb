// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb::ReifyDB;
use reifydb::network::ws::server::WsConfig;
use std::thread;
use tokio::runtime::Runtime;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::oneshot;
use tokio::{select, signal};

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
    ReifyDB::server()
        .with_websocket(WsConfig::default())
        .on_create(|ctx| async move {
            ctx.tx("create schema test");
            ctx.tx("create table test.arith(id: int2, value: int2, num: int2)");
            ctx.tx("from [
    { id: 1, value: 1, num: 5  },
    { id: 1, value: 1, num: 10 },
    { id: 1, value: 2, num: 15 },
    { id: 2, value: 1, num: 10 },
    { id: 2, value: 1, num: 30 }
  ] insert test.arith
");
        })
        .serve_blocking(&rt, rx)
        .unwrap();
}
