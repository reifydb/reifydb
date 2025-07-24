// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::frame::Frame;
use reifydb_core::hook::lifecycle::{OnCreateHook, OnStartHook};
use reifydb_core::hook::{BoxedHookIter, Callback};
use reifydb_core::interface::{
    Engine as EngineInterface, Principal, Transaction, UnversionedStorage, VersionedStorage,
};
use reifydb_core::return_hooks;
use reifydb_network::grpc::server::{GrpcConfig, GrpcServer};
use reifydb_network::ws::server::{WsConfig, WsServer};
use std::marker::PhantomData;
use std::net::SocketAddr;
use tokio::runtime::Runtime;
use tokio::select;
use tokio::sync::oneshot::Receiver;
use tokio::task::JoinSet;

struct OnCreateCallback<VS, US, T, E, F>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    E: EngineInterface<VS, US, T>,
    F: Fn(&OnCreateContext<VS, US, T, E>) -> crate::Result<()> + Send + Sync + 'static,
{
    callback: F,
    _phantom: PhantomData<(VS, US, T, E)>,
}

impl<VS, US, T, E, F> Callback<OnCreateHook<VS, US, T, E>> for OnCreateCallback<VS, US, T, E, F>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    E: EngineInterface<VS, US, T>,
    F: Fn(&OnCreateContext<VS, US, T, E>) -> crate::Result<()> + Send + Sync + 'static,
{
    fn on(&self, hook: &OnCreateHook<VS, US, T, E>) -> Result<BoxedHookIter, reifydb_core::Error> {
        let context = OnCreateContext::new(hook.engine.clone());
        (self.callback)(&context)?;
        return_hooks!()
    }
}

pub struct Server<VS, US, T, E>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    E: EngineInterface<VS, US, T>,
{
    pub(crate) engine: E,
    pub(crate) grpc_config: Option<GrpcConfig>,
    pub(crate) grpc: Option<GrpcServer<VS, US, T, E>>,
    pub(crate) ws_config: Option<WsConfig>,
    pub(crate) ws: Option<WsServer<VS, US, T, E>>,
}

impl<VS, US, T, E> Server<VS, US, T, E>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    E: EngineInterface<VS, US, T>,
{
    pub fn with_engine(mut self, engine: E) -> Self {
        self.engine = engine;
        self
    }

    pub fn with_grpc(mut self, config: GrpcConfig) -> Self {
        self.grpc_config = Some(config);
        self
    }

    pub fn with_websocket(mut self, config: WsConfig) -> Self {
        self.ws_config = Some(config);
        self
    }
}

impl<VS, US, T, E> Server<VS, US, T, E>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    E: EngineInterface<VS, US, T>,
{
    pub fn new(engine: E) -> Self {
        Self { engine, grpc_config: None, grpc: None, ws_config: None, ws: None }
    }

    pub fn ws_socket_addr(&self) -> Option<SocketAddr> {
        self.ws.as_ref().and_then(|ws| ws.socket_addr())
    }

    pub fn grpc_socket_addr(&self) -> Option<SocketAddr> {
        self.grpc.as_ref().and_then(|grpc| grpc.socket_addr())
    }

    /// will only be invoked when a new database gets created
    // pub fn on_create_hook<H>(self, hook: OnCreateHook) -> Self {
    //     // self.engine.hooks().lifecycle().on_create().register(Arc::new(hook));
    //     todo!();
    //     self
    // }

    pub fn serve(&mut self, rt: &Runtime) -> crate::Result<()> {
        self.engine.hooks().trigger(OnCreateHook {
            engine: self.engine.clone(),
            _phantom: PhantomData::default(),
        })?; // FIXME this must be triggered by storage
        self.engine.hooks().trigger(OnStartHook {
            engine: self.engine.clone(),
            _phantom: PhantomData::default(),
        })?;

        if let Some(config) = self.ws_config.take() {
            let engine = self.engine.clone();
            let ws = WsServer::new(config, engine.clone());
            self.ws = Some(ws.clone());
            rt.spawn(async move { ws.serve().await.unwrap() });
        };

        if let Some(config) = self.grpc_config.take() {
            let engine = self.engine.clone();
            let grpc = GrpcServer::new(config, engine.clone());
            self.grpc = Some(grpc.clone());
            rt.spawn(async move { grpc.serve().await });
        }

        Ok(())
    }

    pub fn serve_blocking(
        &mut self,
        rt: &Runtime,
        signal: Receiver<()>,
    ) -> Result<(), reifydb_core::Error> {
        self.engine.hooks().trigger(OnCreateHook {
            engine: self.engine.clone(),
            _phantom: PhantomData::default(),
        })?; // FIXME this must be triggered by storage

        self.engine.hooks().trigger(OnStartHook {
            engine: self.engine.clone(),
            _phantom: PhantomData::default(),
        })?;

        rt.block_on(async {
            let mut handles = JoinSet::new();

            if let Some(config) = self.ws_config.take() {
                let engine = self.engine.clone();
                let ws = WsServer::new(config, engine.clone());
                self.ws = Some(ws.clone());
                handles.spawn(
                    async move { ws.serve().await.map_err(|e| format!("WebSocket: {}", e)) },
                );
            };

            if let Some(config) = self.grpc_config.take() {
                let engine = self.engine.clone();
                let grpc = GrpcServer::new(config, engine.clone());
                self.grpc = Some(grpc.clone());
                handles
                    .spawn(async move { grpc.serve().await.map_err(|e| format!("gRPC: {}", e)) });
            }

            loop {
                select! {
                    _ = signal => {
                        println!("shutting down");
                        self.close().await;
                        break;
                    }
                    result = handles.join_next(), if !handles.is_empty() => {
                        match result {
                            Some(Ok(Ok(()))) => {
                                println!("A server completed successfully");
                                break;
                            }
                            Some(Ok(Err(e))) => {
                                eprintln!("Server error: {}", e);
                                break;
                            }
                            Some(Err(e)) => {
                                eprintln!("Server panicked: {}", e);
                                self.close().await;
                                break;
                            }
                            None => {
                                println!("All servers have stopped");
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    pub async fn close(&mut self) {
        if let Some(ws) = self.ws.as_mut() {
            ws.close().await.unwrap();
        }

        if let Some(_grpc) = self.grpc.as_mut() {
            // grpc.close();
        }
    }
}

// struct HookClosure<VS, US, T, F>
// where
//     VS: VersionedStorage,
//     US: UnversionedStorage,
//     T: Transaction<VS, US>,
//     F: Fn(&HookContext<VS, US, T>) -> Result<(), Box<dyn Error>> + Send + Sync + 'static,
// {
//     f: F,
//     _maker: PhantomData<(VS, US, T)>,
// }
//
// impl<VS, US, T, F> OnCreateHook<VS, US, T> for HookClosure<VS, US, T, F>
// where
//     VS: VersionedStorage,
//     US: UnversionedStorage,
//     T: Transaction<VS, US>,
//     F: Fn(&HookContext<VS, US, T>) -> Result<(), Box<dyn Error>> + Send + Sync + 'static,
// {
//     fn on_create(&self, ctx: &HookContext<VS, US, T>) -> Result<(), Box<dyn Error>> {
//         (self.f)(ctx)
//     }
// }
//
pub struct OnCreateContext<VS, US, T, E>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    E: EngineInterface<VS, US, T>,
{
    pub engine: E,
    _phantom: PhantomData<(VS, US, T)>,
}

impl<'a, VS, US, T, E> OnCreateContext<VS, US, T, E>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    E: EngineInterface<VS, US, T>,
{
    pub fn new(engine: E) -> Self {
        Self { engine, _phantom: PhantomData }
    }

    /// Execute a transactional query as root user
    pub fn tx_as_root(&self, rql: &str) -> Result<Vec<Frame>, reifydb_core::Error> {
        let principal = Principal::System { id: 0, name: "root".to_string() };
        self.engine.tx_as(&principal, rql)
    }
    //
    // /// Execute a read-only query as root user
    // pub fn rx_as_root(&self, rql: &str) -> Result<Vec<Frame>, reifydb_core::Error> {
    //     let principal = Principal::System { id: 0, name: "root".to_string() };
    //     self.engine.rx_as(&principal, rql)
    // }

    /// Execute a transactional query as the specified principal
    pub fn tx_as(
        &self,
        principal: &Principal,
        rql: &str,
    ) -> Result<Vec<Frame>, reifydb_core::Error> {
        self.engine.tx_as(principal, rql)
    }
    //
    // /// Execute a read-only query as the specified principal
    // pub fn rx_as(
    //     &self,
    //     principal: &Principal,
    //     rql: &str,
    // ) -> Result<Vec<Frame>, reifydb_core::Error> {
    //     self.engine.rx_as(principal, rql)
    // }
    //
    // /// Get access to the hooks registry for registering callbacks
    // pub fn hooks(&self) -> &Hooks {
    //     self.engine.hooks()
    // }
}

impl<VS, US, T, E> Server<VS, US, T, E>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    E: EngineInterface<VS, US, T>,
{
    pub fn on_create<F>(self, f: F) -> Self
    where
        F: Fn(&OnCreateContext<VS, US, T, E>) -> crate::Result<()> + Send + Sync + 'static,
    {
        let callback = OnCreateCallback { callback: f, _phantom: PhantomData };
        self.engine.hooks().register::<OnCreateHook<VS, US, T, E>, _>(callback);
        self
    }
}
