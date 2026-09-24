// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};
use reifydb_runtime::{shutdown::Shutdown, sync::mutex::Mutex};
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem};
use reifydb_sub_server_ws::acceptor::WsStreamAcceptor;
use tokio::{
	runtime::Handle,
	sync::{oneshot, watch},
};

use crate::session::{self, SessionConfig};

pub struct ConsoleSubsystem {
	running: Arc<AtomicBool>,

	shutdown_tx: Mutex<Option<watch::Sender<bool>>>,

	shutdown_complete_rx: Mutex<Option<oneshot::Receiver<()>>>,

	runtime: Handle,
}

impl ConsoleSubsystem {
	pub(crate) fn new(config: SessionConfig, acceptor: WsStreamAcceptor, runtime: Handle) -> Self {
		let (shutdown_tx, shutdown_rx) = watch::channel(false);
		let (complete_tx, complete_rx) = oneshot::channel();
		let running = Arc::new(AtomicBool::new(true));
		let session_running = running.clone();
		runtime.spawn(async move {
			if let Err(e) = session::run(config, acceptor, shutdown_rx).await {
				panic!("console session failed: {e}");
			}
			session_running.store(false, Ordering::SeqCst);
			let _ = complete_tx.send(());
		});
		Self {
			running,
			shutdown_tx: Mutex::new(Some(shutdown_tx)),
			shutdown_complete_rx: Mutex::new(Some(complete_rx)),
			runtime,
		}
	}
}

impl HasVersion for ConsoleSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Console subsystem that dials the tunnel server and serves its streams"
				.to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}

impl Shutdown for ConsoleSubsystem {
	fn shutdown(&self) {
		if let Some(tx) = self.shutdown_tx.lock().take() {
			let _ = tx.send(true);
		}

		let complete_rx = self.shutdown_complete_rx.lock().take();
		if let Some(rx) = complete_rx {
			let _ = self.runtime.block_on(rx);
		}
		self.running.store(false, Ordering::SeqCst);
	}
}

impl Subsystem for ConsoleSubsystem {
	fn name(&self) -> &'static str {
		"console"
	}

	fn is_running(&self) -> bool {
		self.running.load(Ordering::SeqCst)
	}

	fn health_status(&self) -> HealthStatus {
		if self.running.load(Ordering::SeqCst) {
			HealthStatus::Healthy
		} else {
			HealthStatus::Failed {
				description: "Not running".to_string(),
			}
		}
	}

	fn as_any(&self) -> &dyn Any {
		self
	}
}
