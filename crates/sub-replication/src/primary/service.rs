// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{ops::Bound, sync::Arc};

use reifydb_cdc::storage::CdcStore;
use reifydb_core::common::CommitVersion;
use tokio::{
	spawn,
	sync::{Notify, mpsc},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, error};

use crate::{
	convert::cdc_to_proto,
	generated::{
		CdcEntry, GetVersionRequest, GetVersionResponse, StreamCdcRequest,
		reify_db_replication_server::ReifyDbReplication,
	},
};

pub struct ReplicationService {
	cdc_store: Arc<CdcStore>,
	notify: Arc<Notify>,
	batch_size: u64,
}

impl ReplicationService {
	pub fn new(cdc_store: CdcStore, notify: Arc<Notify>, batch_size: u64) -> Self {
		Self {
			cdc_store: Arc::new(cdc_store),
			notify,
			batch_size,
		}
	}
}

#[tonic::async_trait]
impl ReifyDbReplication for ReplicationService {
	type StreamCdcStream = ReceiverStream<Result<CdcEntry, Status>>;

	async fn stream_cdc(
		&self,
		request: Request<StreamCdcRequest>,
	) -> Result<Response<Self::StreamCdcStream>, Status> {
		let req = request.into_inner();
		let since = CommitVersion(req.since_version);
		let batch_size = if req.batch_size > 0 {
			req.batch_size
		} else {
			self.batch_size
		};

		let (tx, rx) = mpsc::channel(256);
		let store = self.cdc_store.clone();
		let notify = self.notify.clone();

		debug!(since_version = since.0, "Replica connected for CDC streaming");

		spawn(async move {
			let mut cursor = since;

			loop {
				let batch = store.read_range(Bound::Excluded(cursor), Bound::Unbounded, batch_size);

				match batch {
					Ok(batch) if !batch.items.is_empty() => {
						for cdc in &batch.items {
							let entry = cdc_to_proto(cdc);
							cursor = cdc.version;
							if tx.send(Ok(entry)).await.is_err() {
								debug!("Replica disconnected");
								return;
							}
						}
						// If there are more, immediately continue without waiting
						if batch.has_more {
							continue;
						}
					}
					Ok(_) => {
						// No entries available
					}
					Err(e) => {
						error!("CDC read error: {:?}", e);
					}
				}

				// Wait for notification that new CDC entries were written
				notify.notified().await;
			}
		});

		Ok(Response::new(ReceiverStream::new(rx)))
	}

	async fn get_version(
		&self,
		_request: Request<GetVersionRequest>,
	) -> Result<Response<GetVersionResponse>, Status> {
		let current = self
			.cdc_store
			.max_version()
			.map_err(|e| Status::internal(format!("Failed to get max version: {:?}", e)))?
			.map(|v| v.0)
			.unwrap_or(0);

		let min = self
			.cdc_store
			.min_version()
			.map_err(|e| Status::internal(format!("Failed to get min version: {:?}", e)))?
			.map(|v| v.0)
			.unwrap_or(0);

		let max = current;

		Ok(Response::new(GetVersionResponse {
			current_version: current,
			min_cdc_version: min,
			max_cdc_version: max,
		}))
	}
}
