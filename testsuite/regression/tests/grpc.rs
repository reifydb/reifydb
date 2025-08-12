// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb::core::hook::Hooks;
use reifydb::core::interface::{Params, UnversionedTransaction, VersionedTransaction};
use reifydb::core::retry;
use reifydb::network::grpc::client::GrpcClient;
use reifydb::network::grpc::server::GrpcConfig;
use reifydb::{Database, ServerBuilder, memory, optimistic};
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
    instance: Option<Database<VT, UT>>,
    client: Option<GrpcClient>,
    runtime: Option<Runtime>,
}

impl<VT, UT> GrpcRunner<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub fn new(input: (VT, UT, Hooks)) -> Self {
        let (versioned, unversioned, hooks) = input;
        let instance = ServerBuilder::new(versioned, unversioned, hooks)
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
                    for frame in self.client.as_ref().unwrap().command(&rql, Params::None).await? {
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
                    for frame in self.client.as_ref().unwrap().query(&rql, Params::None).await? {
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
        let server = self.instance.as_mut().unwrap();
        server.start()?;
        let socket_addr = busy_wait(|| server.grpc_socket_addr());

        self.client = Some(GrpcClient { socket_addr });
        self.runtime = Some(runtime);

        Ok(())
    }

    fn end_script(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(mut server) = self.instance.take() {
            let _ = server.stop();
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
