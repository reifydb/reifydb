// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb::client::Client;
use reifydb::interface::{Transaction, UnversionedStorage, VersionedStorage};
use reifydb::server::{DatabaseConfig, Server, ServerConfig};
use reifydb::{ReifyDB, lmdb, memory, optimistic, retry, serializable, sqlite};
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

pub struct ClientRunner<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    server: Option<Server<VS, US, T>>,
    client: Client,
    runtime: Option<Runtime>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl<VS, US, T> ClientRunner<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn new(transaction: T) -> Self {
        let socket_addr = free_local_socket();

        let server = ReifyDB::server_with(transaction).with_config(ServerConfig {
            database: DatabaseConfig { socket_addr: Some(socket_addr) },
        });

        let client = Client { socket_addr };

        Self { server: Some(server), client, runtime: None, shutdown: None }
    }
}

impl<VS, US, T> testscript::Runner for ClientRunner<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
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
                    for line in self.client.tx(&query).await? {
                        writeln!(output, "{}", line).unwrap();
                    }
                    Ok::<(), reifydb::Error>(())
                })?;
            }

            "rx" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("rx: {query}");

                let Some(runtime) = &self.runtime else { panic!() };

                runtime.block_on(async {
                    for line in self.client.rx(&query).await? {
                        writeln!(output, "{}", line).unwrap();
                    }
                    Ok::<(), reifydb::Error>(())
                })?;
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
        let (shutdown_tx, _) = oneshot::channel();
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

test_each_path! { in "testsuite/regression/tests/scripts" as optimistic_memory => test_optimistic_memory }
test_each_path! { in "testsuite/regression/tests/scripts" as optimistic_lmdb => test_optimistic_lmdb }
test_each_path! { in "testsuite/regression/tests/scripts" as optimistic_sqlite => test_optimistic_sqlite }

fn test_optimistic_memory(path: &Path) {
    retry(3, || testscript::run_path(&mut ClientRunner::new(optimistic(memory())), path))
        .expect("test failed")
}

fn test_optimistic_lmdb(path: &Path) {
    temp_dir(|db_path| {
        retry(3, || testscript::run_path(&mut ClientRunner::new(optimistic(lmdb(db_path))), path))
            .expect("test failed")
    })
}

fn test_optimistic_sqlite(path: &Path) {
    temp_dir(|db_path| {
        retry(3, || testscript::run_path(&mut ClientRunner::new(optimistic(sqlite(db_path))), path))
            .expect("test failed")
    })
}

test_each_path! { in "testsuite/regression/tests/scripts" as serializable_memory => test_serializable_memory }
test_each_path! { in "testsuite/regression/tests/scripts" as serializable_lmdb => test_serializable_lmdb }
test_each_path! { in "testsuite/regression/tests/scripts" as serializable_sqlite => test_serializable_sqlite }

fn test_serializable_memory(path: &Path) {
    retry(3, || testscript::run_path(&mut ClientRunner::new(serializable(memory())), path))
        .expect("test failed")
}

fn test_serializable_lmdb(path: &Path) {
    temp_dir(|db_path| {
        retry(3, || testscript::run_path(&mut ClientRunner::new(serializable(lmdb(db_path))), path))
            .expect("test failed")
    })
}

fn test_serializable_sqlite(path: &Path) {
    temp_dir(|db_path| {
        retry(3, || {
            testscript::run_path(&mut ClientRunner::new(serializable(sqlite(db_path))), path)
        })
        .expect("test failed")
    })
}
