// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{error::Error, time::Duration};

use tokio::{select, sync::watch, task::block_in_place, time::sleep};
use tracing::{debug, error, info, warn};

use super::applier::ReplicaApplier;
use crate::generated::{StreamCdcRequest, reify_db_replication_client::ReifyDbReplicationClient};

/// Client that connects to a primary and replicates CDC entries.
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

	/// Run the replication loop until shutdown is signaled.
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

		let mut client = select! {
		    result = ReifyDbReplicationClient::connect(self.primary_addr.clone()) => result?,
		    _ = shutdown_rx.changed() => {
			return Ok(());
		    }
		};

		let request = StreamCdcRequest {
			since_version,
			batch_size: self.batch_size,
		};

		let response = select! {
		    result = client.stream_cdc(request) => result?,
		    _ = shutdown_rx.changed() => {
			return Ok(());
		    }
		};
		let mut stream = response.into_inner();

		debug!("Replication stream established");

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
					// Stream ended
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
