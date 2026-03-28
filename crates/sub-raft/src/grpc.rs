// Copyright (c) 2025 ReifyDB
// SPDX-License-Identifier: Apache-2.0

use std::{
	collections::HashMap,
	error::Error,
	mem::take,
	net::SocketAddr,
	sync::{Arc, Mutex},
	time::Duration,
};

use postcard::{from_bytes, to_stdvec};
use tokio::{spawn, sync::mpsc, task::JoinHandle, time::sleep};
use tonic::{Request, Response, Status, transport::Server};

use crate::{
	config::PeerConfig,
	generated::raft_v1::{
		RaftAck, RaftMessage,
		raft_transport_client::RaftTransportClient,
		raft_transport_server::{RaftTransport as RaftTransportTrait, RaftTransportServer},
	},
	message::Envelope,
	node::NodeId,
	transport::Transport,
};

/// gRPC-based transport for Raft messages. Each node runs a gRPC server
/// and maintains client connections to all peers.
pub struct GrpcTransport {
	node_id: NodeId,
	inbound: Arc<Mutex<Vec<Envelope>>>,
	outbound_txs: HashMap<NodeId, mpsc::UnboundedSender<Envelope>>,
}

impl GrpcTransport {
	/// Start the gRPC server and connect to all peers.
	/// Returns the transport and a future that runs the server.
	pub async fn start(
		node_id: NodeId,
		bind_addr: SocketAddr,
		peers: Vec<PeerConfig>,
	) -> Result<(Self, JoinHandle<()>), Box<dyn Error>> {
		let inbound = Arc::new(Mutex::new(Vec::new()));

		// Start gRPC server.
		let service = InboundService {
			inbound: inbound.clone(),
		};
		let server_handle = {
			let addr = bind_addr;
			spawn(async move {
				Server::builder()
					.add_service(RaftTransportServer::new(service))
					.serve(addr)
					.await
					.expect("raft gRPC server failed");
			})
		};

		// Give server a moment to bind.
		sleep(Duration::from_millis(50)).await;

		// Connect to each peer.
		let mut outbound_txs = HashMap::new();
		for peer in &peers {
			let (tx, mut rx) = mpsc::unbounded_channel::<Envelope>();
			outbound_txs.insert(peer.node_id, tx);

			let addr = format!("http://{}", peer.addr);
			spawn(async move {
				loop {
					match RaftTransportClient::connect(addr.clone()).await {
						Ok(mut client) => {
							while let Some(envelope) = rx.recv().await {
								let payload = to_stdvec(&envelope)
									.expect("serialize envelope");
								let msg = RaftMessage {
									payload,
								};
								if client.send(msg).await.is_err() {
									break;
								}
							}
						}
						Err(_) => {
							sleep(Duration::from_millis(500)).await;
						}
					}
				}
			});
		}

		let transport = Self {
			node_id,
			inbound,
			outbound_txs,
		};
		Ok((transport, server_handle))
	}
}

impl Transport for GrpcTransport {
	fn send(&self, envelope: Envelope) {
		if let Some(tx) = self.outbound_txs.get(&envelope.to) {
			let _ = tx.send(envelope);
		}
	}

	fn receive(&self) -> Vec<Envelope> {
		let mut inbound = self.inbound.lock().unwrap();
		take(&mut *inbound)
	}
}

struct InboundService {
	inbound: Arc<Mutex<Vec<Envelope>>>,
}

#[tonic::async_trait]
impl RaftTransportTrait for InboundService {
	async fn send(&self, request: Request<RaftMessage>) -> Result<Response<RaftAck>, Status> {
		let msg = request.into_inner();
		let envelope: Envelope =
			from_bytes(&msg.payload).map_err(|e| Status::invalid_argument(format!("deserialize: {e}")))?;
		self.inbound.lock().unwrap().push(envelope);
		Ok(Response::new(RaftAck {}))
	}
}
