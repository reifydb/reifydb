// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb::client::GrpcClient;
use reifydb::core::hook::Hooks;
use reifydb::core::interface::{VersionedTransaction, UnversionedTransaction};
use reifydb::core::retry;
use reifydb::network::grpc::server::GrpcConfig;
use reifydb::variant::server::Server;
use reifydb::{ReifyDB, memory, optimistic};
use reifydb_testing::network::busy_wait;
use reifydb_testing::testscript;
use reifydb_testing::testscript::Command;
use std::error::Error;
use std::fmt::Write;
use std::path::Path;
use test_each_file::test_each_path;
use tokio::runtime::Runtime;

pub struct GrpcRunner<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,

{
    instance: Option<Server<VT, UT>>,
    client: Option<GrpcClient>,
    runtime: Option<Runtime>,
}

impl<VT, UT> GrpcRunner<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,

{
    pub fn new(input: (VT, UT, Hooks)) -> Self {
        let instance = ReifyDB::server_with(input)
            .with_grpc(GrpcConfig { socket: Some("[::1]:0".parse().unwrap()) })
            .build();

        Self { instance: Some(instance), client: None, runtime: None }
    }
}

impl<VT, UT> testscript::Runner for GrpcRunner<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,

{
    fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
        let mut output = String::new();
        match command.name.as_str() {
            "command" => {
                let rql =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("command: {rql}");

                let Some(runtime) = &self.runtime else { panic!() };

                runtime.block_on(async {
                    for frame in self.client.as_ref().unwrap().command(&rql).await? {
                        writeln!(output, "{}", frame).unwrap();
                    }
                    Ok::<(), reifydb::Error>(())
                })?;
            }

            "query" => {
                let rql =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("query: {rql}");

                let Some(runtime) = &self.runtime else { panic!() };

                runtime.block_on(async {
                    for frame in self.client.as_ref().unwrap().query(&rql).await? {
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
        let mut server = self.instance.take().unwrap();
        let _ = server.serve(&runtime);
        let socket_addr = busy_wait(|| server.grpc_socket_addr());

        self.instance = Some(server);
        self.client = Some(GrpcClient { socket_addr });
        self.runtime = Some(runtime);

        Ok(())
    }

    fn end_script(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(server) = self.instance.take() {
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
