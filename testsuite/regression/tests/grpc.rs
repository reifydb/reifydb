// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb::client::GrpcClient;
use reifydb::core::hook::Hooks;
use reifydb::core::interface::{Transaction, UnversionedStorage, VersionedStorage};
use reifydb::core::retry;
use reifydb::network::grpc::server::GrpcConfig;
use reifydb::server::Server;
use reifydb::{ReifyDB, memory, optimistic};
use reifydb_testing::network::busy_wait;
use reifydb_testing::testscript;
use reifydb_testing::testscript::Command;
use std::error::Error;
use std::fmt::Write;
use std::path::Path;
use test_each_file::test_each_path;
use tokio::runtime::Runtime;

pub struct GrpcRunner<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    server: Option<Server<VS, US, T>>,
    client: Option<GrpcClient>,
    runtime: Option<Runtime>,
}

impl<VS, US, T> GrpcRunner<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn new(input: (T, Hooks)) -> Self {
        let engine = ReifyDB::server_with(input)
            .with_grpc(GrpcConfig { socket: Some("[::1]:0".parse().unwrap()) });

        Self { server: Some(engine), client: None, runtime: None }
    }
}

impl<VS, US, T> testscript::Runner for GrpcRunner<VS, US, T>
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
                    for frame in self.client.as_ref().unwrap().tx(&query).await? {
                        writeln!(output, "{}", frame).unwrap();
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
                    for frame in self.client.as_ref().unwrap().rx(&query).await? {
                        writeln!(output, "{}", frame).unwrap();
                    }
                    Ok::<(), reifydb::Error>(())
                })?;
            }
            name => return Err(format!("invalid command {name}").into()),
        }

        Ok(output)
    }

    fn start_script(&mut self) -> Result<(), Box<dyn Error>> {
        let runtime = Runtime::new()?;
        let mut server = self.server.take().unwrap();
        let _ = server.serve(&runtime);
        let socket_addr = busy_wait(|| server.grpc_socket_addr());

        self.server = Some(server);
        self.client = Some(GrpcClient { socket_addr });
        self.runtime = Some(runtime);

        Ok(())
    }

    fn end_script(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(server) = self.server.take() {
            drop(server);
        }

        if let Some(runtime) = self.runtime.take() {
            drop(runtime);
        }

        Ok(())
    }
}

test_each_path! { in "testsuite/regression/tests/scripts" as grpc => test_grpc }

fn test_grpc(path: &Path) {
    retry(3, || testscript::run_path(&mut GrpcRunner::new(optimistic(memory())), path))
        .expect("test failed")
}
