// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb::core::hook::Hooks;
use reifydb::core::interface::{UnversionedTransaction, VersionedTransaction};
use reifydb::core::{Error as ReifyDBError, retry};
use reifydb::network::ws::client::WsClient;
use reifydb::network::ws::server::WsConfig;
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
use tokio::sync::oneshot;

pub struct WsRunner<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    instance: Option<Server<VT, UT>>,
    client: Option<WsClient>,
    runtime: Option<Runtime>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl<VT, UT> WsRunner<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub fn new(input: (VT, UT, Hooks)) -> Self {
        let instance = ReifyDB::server_with(input)
            .with_websocket(WsConfig { socket: Some("[::1]:0".parse().unwrap()) })
            .build();

        Self { instance: Some(instance), client: None, runtime: None, shutdown: None }
    }
}

impl<VT, UT> testscript::Runner for WsRunner<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
        let mut output = String::new();
        match command.name.as_str() {
            "write" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("tx: {query}");

                let Some(runtime) = &self.runtime else { panic!() };

                runtime.block_on(async {
                    for frame in self.client.as_ref().unwrap().write(&query).await? {
                        writeln!(output, "{}", frame).unwrap();
                    }
                    Ok::<(), reifydb::Error>(())
                })?;
            }

            "read" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("rx: {query}");

                let Some(runtime) = &self.runtime else { panic!() };

                runtime.block_on(async {
                    for frame in self.client.as_ref().unwrap().read(&query).await? {
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
        let mut server = self.instance.take().unwrap();

        let _ = server.serve(&runtime);
        let socket_addr = busy_wait(|| server.ws_socket_addr());

        self.instance = Some(server);
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
        if let Some(server) = self.instance.take() {
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

test_each_path! { in "testsuite/regression/tests/scripts" as websocket => test_websocket }

fn test_websocket(path: &Path) {
    retry(3, || testscript::run_path(&mut WsRunner::new(optimistic(memory())), path))
        .expect("test failed")
}
