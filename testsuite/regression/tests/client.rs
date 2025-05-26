// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb::client::Client;
use reifydb::server::{DatabaseConfig, Server, ServerConfig};
use reifydb::store::Store;
use reifydb::transaction::TransactionEngine;
use reifydb::{ReifyDB, memory, mvcc, svl};
use std::error::Error;
use std::fmt::Write;
use std::path::Path;
use test_each_file::test_each_path;
use testing::network::free_local_socket;
use testing::testscript;
use testing::testscript::Command;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

pub struct ClientRunner<S: Store, T: TransactionEngine<S>> {
    server: Option<Server<S, T>>,
    client: Client,
    runtime: Option<Runtime>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl<S: Store + 'static, T: TransactionEngine<S> + 'static> ClientRunner<S, T> {
    pub fn new(transaction: T) -> Self {
        let socket_addr = free_local_socket();

        let server = ReifyDB::server_with(transaction).with_config(ServerConfig {
            database: DatabaseConfig { socket_addr: Some(socket_addr.clone()) },
        });

        let client = Client { socket_addr };

        Self { server: Some(server), client, runtime: None, shutdown: None }
    }
}

impl<S: Store + 'static, T: TransactionEngine<S> + 'static> testscript::Runner
    for ClientRunner<S, T>
{
    fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
        let mut output = String::new();
        match command.name.as_str() {
            "tx" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("tx: {query}");

                let Some(runtime) = &self.runtime else { panic!() };

                runtime.block_on(async {
                    for line in self.client.tx(&query).await {
                        writeln!(output, "{}", line).unwrap();
                    }
                });
            }

            "rx" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("rx: {query}");

                let Some(runtime) = &self.runtime else { panic!() };

                runtime.block_on(async {
                    for line in self.client.rx(&query).await {
                        writeln!(output, "{}", line).unwrap();
                    }
                });
            }
            "list_schema" => {
                writeln!(output, "test")?;
            }
            name => return Err(format!("invalid command {name}").into()),
        }

        Ok(output)
    }

    fn start_script(&mut self) -> Result<(), Box<dyn Error>> {
        let runtime = Runtime::new()?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = self.server.take().unwrap();

        runtime.spawn(async move {
            let _ = server.serve().await;
        });

        self.runtime = Some(runtime);
        self.shutdown = Some(shutdown_tx);

        Ok(())
    }

    fn end_script(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }

        if let Some(runtime) = self.runtime.take() {
            drop(runtime);
        }

        Ok(())
    }
}

test_each_path! { in "testsuite/regression/tests/scripts" as client_svl_memory => test_svl_memory }
test_each_path! { in "testsuite/regression/tests/scripts" as client_mvcc_memory => test_mvcc_memory }

fn test_svl_memory(path: &Path) {
    testscript::run_path(&mut ClientRunner::new(svl(memory())), path).expect("test failed")
}

fn test_mvcc_memory(path: &Path) {
    testscript::run_path(&mut ClientRunner::new(mvcc(memory())), path).expect("test failed")
}
