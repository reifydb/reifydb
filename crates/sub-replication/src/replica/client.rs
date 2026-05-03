// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{error::Error, time::Duration};

use tokio::{select, sync::watch, task::block_in_place, time::sleep};
use tonic::{Streaming, transport::Channel};
use tracing::{debug, error, info, warn};

use super::applier::ReplicaApplier;
use crate::generated::{CdcEntry, StreamCdcRequest, reify_db_replication_client::ReifyDbReplicationClient};

pub struct ReplicationClient {
	primary_addr: String,
	applier: ReplicaApplier,
	reconnect_interval: Duration,
	batch_size: u64,
}

impl ReplicationClient {
	pub fn new(
		primary_addr: String,
		applier: ReplicaApplier,
		reconnect_interval: Duration,
		batch_size: u64,
	) -> Self {
		Self {
			primary_addr,
			applier,
			reconnect_interval,
			batch_size,
		}
	}

	pub async fn run(self, mut shutdown_rx: watch::Receiver<bool>) {
		loop {
			if *shutdown_rx.borrow() {
				info!("Replication client shutting down");
				return;
			}

			match self.connect_and_stream(&mut shutdown_rx).await {
				Ok(()) => {
					info!("Replication stream ended cleanly");
					return;
				}
				Err(e) => {
					warn!(
						"Replication stream disconnected: {}, reconnecting in {:?}...",
						e, self.reconnect_interval
					);
					select! {
					    _ = sleep(self.reconnect_interval) => {}
					    _ = shutdown_rx.changed() => {
						if *shutdown_rx.borrow() {
						    info!("Replication client shutting down during reconnect");
						    return;
						}
					    }
					}
				}
			}
		}
	}

	async fn connect_and_stream(
		&self,
		shutdown_rx: &mut watch::Receiver<bool>,
	) -> Result<(), Box<dyn Error + Send + Sync>> {
		let since_version = self.applier.current_version().0;
		info!(
		    primary = %self.primary_addr,
		    since_version,
		    "Connecting to primary for replication"
		);

		let Some(mut client) = self.connect_to_primary(shutdown_rx).await? else {
			return Ok(());
		};
		let Some(mut stream) = self.open_cdc_stream(&mut client, since_version, shutdown_rx).await? else {
			return Ok(());
		};
		debug!("Replication stream established");
		self.apply_stream_entries(&mut stream, shutdown_rx).await
	}

	async fn connect_to_primary(
		&self,
		shutdown_rx: &mut watch::Receiver<bool>,
	) -> Result<Option<ReifyDbReplicationClient<Channel>>, Box<dyn Error + Send + Sync>> {
		select! {
		    result = ReifyDbReplicationClient::connect(self.primary_addr.clone()) => Ok(Some(result?)),
		    _ = shutdown_rx.changed() => Ok(None),
		}
	}

	async fn open_cdc_stream(
		&self,
		client: &mut ReifyDbReplicationClient<Channel>,
		since_version: u64,
		shutdown_rx: &mut watch::Receiver<bool>,
	) -> Result<Option<Streaming<CdcEntry>>, Box<dyn Error + Send + Sync>> {
		let request = StreamCdcRequest {
			since_version,
			batch_size: self.batch_size,
		};
		select! {
		    result = client.stream_cdc(request) => Ok(Some(result?.into_inner())),
		    _ = shutdown_rx.changed() => Ok(None),
		}
	}

	async fn apply_stream_entries(
		&self,
		stream: &mut Streaming<CdcEntry>,
		shutdown_rx: &mut watch::Receiver<bool>,
	) -> Result<(), Box<dyn Error + Send + Sync>> {
		loop {
			select! {
			    msg = stream.message() => {
				match msg {
				    Ok(Some(entry)) => {
					let applier = &self.applier;
					let result = block_in_place(|| applier.apply(&entry));
					if let Err(e) = result {
					    error!(version = entry.version, "Failed to apply CDC entry: {:?}", e);
					    return Err(e.into());
					}
				    }
				    Ok(None) => {
					debug!("Replication stream ended");
					return Err("stream ended".into());
				    }
				    Err(e) => {
					error!("Replication stream error: {:?}", e);
					return Err(e.into());
				    }
				}
			    }
			    _ = shutdown_rx.changed() => {
				if *shutdown_rx.borrow() {
				    return Ok(());
				}
			    }
			}
		}
	}
}
