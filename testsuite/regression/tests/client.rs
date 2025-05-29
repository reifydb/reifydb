// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb::client::Client;
use reifydb::reifydb_persistence::{Lmdb, Memory, Persistence};
use reifydb::reifydb_transaction::Transaction;
use reifydb::reifydb_transaction::mvcc::transaction::optimistic::OptimisticDb;
use reifydb::reifydb_transaction::mvcc::transaction::serializable::SerializableDb;
use reifydb::server::{DatabaseConfig, Server, ServerConfig};
use reifydb::{ReifyDB, memory, mvcc, optimistic, serializable, svl};
use reifydb_testing::network::free_local_socket;
use reifydb_testing::tempdir::temp_dir;
use reifydb_testing::testscript;
use reifydb_testing::testscript::Command;
use std::error::Error;
use std::fmt::Write;
use std::path::Path;
use test_each_file::test_each_path;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

pub struct ClientRunner<P: Persistence, T: Transaction<P>> {
    server: Option<Server<P, T>>,
    client: Client,
    runtime: Option<Runtime>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl<P: Persistence + 'static, T: Transaction<P> + 'static> ClientRunner<P, T> {
    pub fn new(transaction: T) -> Self {
        let socket_addr = free_local_socket();

        let server = ReifyDB::server_with(transaction).with_config(ServerConfig {
            database: DatabaseConfig { socket_addr: Some(socket_addr.clone()) },
        });

        let client = Client { socket_addr };

        Self { server: Some(server), client, runtime: None, shutdown: None }
    }
}

impl<P: Persistence + 'static, T: Transaction<P> + 'static> testscript::Runner
    for ClientRunner<P, T>
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

test_each_path! { in "testsuite/regression/tests/scripts" as client_serializable_memory => test_serializable_memory }
test_each_path! { in "testsuite/regression/tests/scripts" as client_optimistic_memory => test_optimistic_memory }

test_each_path! { in "testsuite/regression/tests/scripts" as client_svl_memory => test_svl_memory }
test_each_path! { in "testsuite/regression/tests/scripts" as client_mvcc_memory => test_mvcc_memory }

test_each_path! { in "testsuite/regression/tests/scripts" as client_svl_lmdb => test_svl_lmdb }
test_each_path! { in "testsuite/regression/tests/scripts" as client_mvcc_lmdb => test_mvcc_lmdb }

fn test_serializable_memory(path: &Path) {
    testscript::run_path(
        &mut ClientRunner::<Memory, SerializableDb<Vec<u8>, Vec<u8>>>::new(serializable()),
        path,
    )
    .expect("test failed")
}

fn test_optimistic_memory(path: &Path) {
    testscript::run_path(
        &mut ClientRunner::<Memory, OptimisticDb<Vec<u8>, Vec<u8>>>::new(optimistic()),
        path,
    )
    .expect("test failed")
}

fn test_mvcc_memory(path: &Path) {
    testscript::run_path(&mut ClientRunner::new(mvcc(memory())), path).expect("test failed")
}

fn test_svl_memory(path: &Path) {
    testscript::run_path(&mut ClientRunner::new(svl(memory())), path).expect("test failed")
}

fn test_mvcc_lmdb(path: &Path) {
    temp_dir(|db_path| {
        testscript::run_path(&mut ClientRunner::new(mvcc(Lmdb::new(db_path).unwrap())), path)
            .expect("test failed")
    })
}

fn test_svl_lmdb(path: &Path) {
    temp_dir(|db_path| {
        testscript::run_path(&mut ClientRunner::new(svl(Lmdb::new(db_path).unwrap())), path)
            .expect("test failed")
    })
}
