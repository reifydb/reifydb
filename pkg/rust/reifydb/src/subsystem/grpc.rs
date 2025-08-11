// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::context::RuntimeProvider;
use crate::health::HealthStatus;
use crate::subsystem::Subsystem;
use reifydb_core::interface::{UnversionedTransaction, VersionedTransaction};
use reifydb_core::Result;
use reifydb_network::grpc::server::{GrpcConfig, GrpcServer};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
#[cfg(feature = "async")]
use tokio::task::JoinHandle;

/// Adapter to make GrpcServer compatible with the Subsystem trait
///
/// This wrapper implements the Subsystem trait for GrpcServer, allowing
/// it to be managed by the Database architecture. It handles the
/// async-to-sync bridge for the gRPC server lifecycle.
pub struct GrpcSubsystemAdapter<VT: VersionedTransaction, UT: UnversionedTransaction> {
    /// The wrapped GrpcServer
    grpc_server: Option<GrpcServer<VT, UT>>,
    /// Subsystem name
    name: String,
    /// Whether the server is running
    running: Arc<AtomicBool>,
    /// Handle to the async task
    #[cfg(feature = "async")]
    task_handle: Option<JoinHandle<()>>,
    /// Shared runtime provider
    runtime_provider: RuntimeProvider,
}

impl<VT: VersionedTransaction, UT: UnversionedTransaction> GrpcSubsystemAdapter<VT, UT> {
    /// Create a new GrpcServer adapter with shared runtime
    pub fn new(
        config: GrpcConfig, 
        engine: reifydb_engine::Engine<VT, UT>,
        runtime_provider: &RuntimeProvider,
    ) -> Self {
        let grpc_server = GrpcServer::new(config, engine);
        Self {
            grpc_server: Some(grpc_server),
            name: "grpc".to_string(),
            running: Arc::new(AtomicBool::new(false)),
            #[cfg(feature = "async")]
            task_handle: None,
            runtime_provider: runtime_provider.clone(),
        }
    }

    /// Create a new GrpcServer adapter with custom name and shared runtime
    pub fn with_name(
        config: GrpcConfig, 
        engine: reifydb_engine::Engine<VT, UT>, 
        name: String,
        runtime_provider: &RuntimeProvider,
    ) -> Self {
        let grpc_server = GrpcServer::new(config, engine);
        Self {
            grpc_server: Some(grpc_server),
            name,
            running: Arc::new(AtomicBool::new(false)),
            #[cfg(feature = "async")]
            task_handle: None,
            runtime_provider: runtime_provider.clone(),
        }
    }

    /// Get the socket address if the server is running
    pub fn socket_addr(&self) -> Option<std::net::SocketAddr> {
        self.grpc_server.as_ref().and_then(|server| server.socket_addr())
    }
}

impl<VT, UT> Subsystem for GrpcSubsystemAdapter<VT, UT>
where
    VT: VersionedTransaction + Send + Sync + 'static,
    UT: UnversionedTransaction + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn start(&mut self) -> Result<()> {
        if self.running.load(Ordering::Relaxed) {
            return Ok(()); // Already running
        }

        if let Some(server) = self.grpc_server.take() {
            let running = Arc::clone(&self.running);
            
            // Use shared runtime to spawn the server
            let handle = self.runtime_provider.spawn(async move {
                running.store(true, Ordering::Relaxed);
                println!("[GrpcSubsystem] Starting gRPC server");
                
                if let Err(e) = server.serve().await {
                    eprintln!("[GrpcSubsystem] gRPC server error: {}", e);
                }
                
                running.store(false, Ordering::Relaxed);
                println!("[GrpcSubsystem] gRPC server stopped");
            });

            // Give the server a moment to start
            std::thread::sleep(std::time::Duration::from_millis(100));
            
            #[cfg(feature = "async")]
            {
                self.task_handle = Some(handle);
            }
        }

        self.running.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        if !self.running.load(Ordering::Relaxed) {
            return Ok(()); // Already stopped
        }

        self.running.store(false, Ordering::Relaxed);
        
        // Clean up task handle
        #[cfg(feature = "async")]
        {
            if let Some(handle) = self.task_handle.take() {
                handle.abort();
            }
        }

        println!("[GrpcSubsystem] gRPC server stopped");
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    fn health_status(&self) -> HealthStatus {
        if self.is_running() {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unknown
        }
    }
}