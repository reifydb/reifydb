// Copyright (c) 2025 ReifyDB
// SPDX-License-Identifier: Apache-2.0

//! Starts a single Raft cluster node with gRPC transport.
//!
//! Usage:
//!   raft-cluster --node-id 1 --bind 127.0.0.1:9100 \
//!     --peer 2=127.0.0.1:9200 --peer 3=127.0.0.1:9300

use std::{collections::HashSet, net::SocketAddr};

use reifydb_sub_raft::{
	KVState, Log, Node, Options,
	config::PeerConfig,
	driver::{DriverConfig, RaftDriver},
	grpc::GrpcTransport,
	node::NodeId,
};

#[tokio::main]
async fn main() {
	let args: Vec<String> = std::env::args().collect();

	let mut node_id: Option<NodeId> = None;
	let mut bind_addr: Option<SocketAddr> = None;
	let mut peers: Vec<PeerConfig> = Vec::new();

	let mut i = 1;
	while i < args.len() {
		match args[i].as_str() {
			"--node-id" => {
				i += 1;
				node_id = Some(args[i].parse().expect("invalid node-id"));
			}
			"--bind" => {
				i += 1;
				bind_addr = Some(args[i].parse().expect("invalid bind address"));
			}
			"--peer" => {
				i += 1;
				let (id_str, addr_str) = args[i].split_once('=').expect("peer format: ID=ADDR");
				peers.push(PeerConfig {
					node_id: id_str.parse().expect("invalid peer node-id"),
					addr: addr_str.parse().expect("invalid peer address"),
				});
			}
			other => panic!("unknown argument: {other}"),
		}
		i += 1;
	}

	let node_id = node_id.expect("--node-id required");
	let bind_addr = bind_addr.expect("--bind required");

	let peer_ids: HashSet<NodeId> = peers.iter().map(|p| p.node_id).collect();
	let opts = Options::default();
	let log = Log::new();
	let state = Box::new(KVState::new());
	let node = Node::new_seeded(node_id, peer_ids, log, state, opts, node_id as u64);

	eprintln!("node {node_id}: starting gRPC transport on {bind_addr}");
	let (transport, _server_handle) =
		GrpcTransport::start(node_id, bind_addr, peers).await.expect("failed to start gRPC transport");

	let config = DriverConfig::default();
	let (driver, handle) = RaftDriver::new(node, transport, config);

	eprintln!("node {node_id}: driver running");
	let driver_handle = tokio::spawn(driver.run());

	// Periodically print status.
	let status_handle = handle.clone();
	tokio::spawn(async move {
		loop {
			tokio::time::sleep(std::time::Duration::from_secs(5)).await;
			eprintln!("node {node_id}: alive (handle active={})", !status_handle.proposal_tx_closed());
		}
	});

	// Run until the driver exits.
	let _ = driver_handle.await;
}
