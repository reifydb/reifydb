// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	net::SocketAddr,
	sync::{Arc, atomic::AtomicUsize},
};

use reifydb_runtime::sync::mutex::Mutex;
use reifydb_sub_server::state::AppState;
use tokio::{
	io::{AsyncRead, AsyncWrite},
	runtime::Handle,
	sync::{Semaphore, watch},
};
use tracing::warn;

use crate::{subscription::registry::SubscriptionRegistry, subsystem::spawn_connection};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Access {
	Query,
	Command,
	Admin,
}

#[derive(Clone)]
pub struct WsStreamAcceptor {
	state: AppState,
	registry: Arc<SubscriptionRegistry>,
	semaphore: Arc<Semaphore>,
	active_connections: Arc<AtomicUsize>,
	shutdown_tx: Arc<Mutex<Option<watch::Sender<bool>>>>,
	runtime: Handle,
}

impl WsStreamAcceptor {
	pub(crate) fn new(
		state: AppState,
		registry: Arc<SubscriptionRegistry>,
		semaphore: Arc<Semaphore>,
		active_connections: Arc<AtomicUsize>,
		shutdown_tx: Arc<Mutex<Option<watch::Sender<bool>>>>,
		runtime: Handle,
	) -> Self {
		Self {
			state,
			registry,
			semaphore,
			active_connections,
			shutdown_tx,
			runtime,
		}
	}

	pub fn accept<S>(&self, stream: S, peer: Option<SocketAddr>, access: Access)
	where
		S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
	{
		let Some(shutdown_rx) = self.shutdown_tx.lock().as_ref().map(|tx| tx.subscribe()) else {
			warn!("ws subsystem is not running, dropping stream from {:?}", peer);
			return;
		};
		spawn_connection(
			stream,
			peer,
			&self.state,
			&self.registry,
			&self.semaphore,
			&self.active_connections,
			&shutdown_rx,
			&self.runtime,
			access,
		);
	}
}
