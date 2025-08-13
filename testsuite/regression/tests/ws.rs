// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb::core::hook::Hooks;
use reifydb::core::interface::{CdcTransaction, StandardTransaction, Params, UnversionedTransaction, VersionedTransaction};
use reifydb::core::{Error as ReifyDBError, retry};
use reifydb::network::ws::client::WsClient;
use reifydb::network::ws::server::WsConfig;
use reifydb::{Database, ServerBuilder, memory, optimistic};
use reifydb_testing::network::busy_wait;
use reifydb_testing::testscript;
use reifydb_testing::testscript::Command;
use std::error::Error;
use std::fmt::Write;
use std::path::Path;
use test_each_file::test_each_path;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

pub struct WsRunner<VT, UT, C>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
    C: CdcTransaction,
{
    instance: Option<Database<StandardTransaction<VT, UT, C>>>,
    client: Option<WsClient>,
    runtime: Option<Runtime>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl<VT, UT, C> WsRunner<VT, UT, C>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
    C: CdcTransaction,
{
    pub fn new(input: (VT, UT, C, Hooks)) -> Self {
        let (versioned, unversioned, cdc, hooks) = input;
        let instance = ServerBuilder::new(versioned, unversioned, cdc, hooks)
            .with_ws(WsConfig { socket: Some("[::1]:0".parse().unwrap()) })
            .build();

        Self { instance: Some(instance), client: None, runtime: None, shutdown: None }
    }
}

impl<VT, UT, C> testscript::Runner for WsRunner<VT, UT, C>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
    C: CdcTransaction,
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
        let (shutdown_tx, _) = oneshot::channel();
        let server = self.instance.as_mut().unwrap();

        server.start()?;
        let socket_addr = busy_wait(|| server.ws_socket_addr());
        self.client = Some(runtime.block_on(async {
            let client = WsClient::connect(&format!("ws://[::1]:{}", socket_addr.port())).await?;
            client.auth(Some("mysecrettoken".into())).await.unwrap();
            Ok::<WsClient, ReifyDBError>(client)
        })?);
        self.runtime = Some(runtime);
        self.shutdown = Some(shutdown_tx);

        Ok(())
    }

    fn end_script(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(mut server) = self.instance.take() {
            let _ = server.stop();
            drop(server);
        }

        if let Some(client) = self.client.take() {
            drop(client);
        }

        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }

        if let Some(runtime) = self.runtime.take() {
            drop(runtime);
        }

        Ok(())
    }
}

test_each_path! { in "testsuite/regression/tests/scripts" as ws => test_ws }

fn test_ws(path: &Path) {
    retry(3, || testscript::run_path(&mut WsRunner::new(optimistic(memory())), path))
        .expect("test failed")
}
