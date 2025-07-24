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
        .on_create(|ctx| {
            ctx.tx_as_root("create schema test")?;
            ctx.tx_as_root("create table test.arith(id: int2, value: int2, num: int2)")?;
            ctx.tx_as_root("insert (1,1,5), (1,1,10), (1,2,15), (2,1,10), (2,1,30) into test.arith(id,value,num)")?;
            Ok(())
        })
        .serve_blocking(&rt, rx)
        .unwrap();
}
