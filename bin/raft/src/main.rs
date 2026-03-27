// Copyright (c) 2025 ReifyDB
// SPDX-License-Identifier: Apache-2.0

//! Spins up a 3-node Raft cluster in a single process with gRPC transport,
//! verifies leader election, proposes writes, and confirms replication.

use std::{collections::HashSet, time::Duration};

use reifydb_sub_raft::{
	KVState, Log, Node, Options,
	config::PeerConfig,
	driver::{DriverConfig, RaftDriver, RaftHandle},
	grpc::GrpcTransport,
	node::NodeId,
	state::test_write,
};

struct ClusterNode {
	id: NodeId,
	handle: RaftHandle,
	driver_join: tokio::task::JoinHandle<()>,
	_server_join: tokio::task::JoinHandle<()>,
}

#[tokio::main]
async fn main() {
	let ports = [19100u16, 19200, 19300];
	let node_ids: Vec<NodeId> = vec![1, 2, 3];

	eprintln!("=== Starting 3-node Raft cluster ===\n");

	let mut nodes: Vec<ClusterNode> = Vec::new();

	for (i, &id) in node_ids.iter().enumerate() {
		let bind_addr = format!("127.0.0.1:{}", ports[i]).parse().unwrap();
		let peers: Vec<PeerConfig> = node_ids
			.iter()
			.enumerate()
			.filter(|(_, pid)| **pid != id)
			.map(|(j, &pid)| PeerConfig {
				node_id: pid,
				addr: format!("127.0.0.1:{}", ports[j]).parse().unwrap(),
			})
			.collect();

		let peer_ids: HashSet<NodeId> = peers.iter().map(|p| p.node_id).collect();
		let opts = Options {
			heartbeat_interval: 3,
			election_timeout_range: 8..15,
			max_append_entries: 100,
		};
		let node = Node::new_seeded(id, peer_ids, Log::new(), Box::new(KVState::new()), opts, id as u64);

		let (transport, server_join) =
			GrpcTransport::start(id, bind_addr, peers).await.expect("failed to start transport");

		let config = DriverConfig {
			tick_interval: Duration::from_millis(50),
			recv_interval: Duration::from_millis(5),
			proposal_channel_capacity: 64,
		};
		let (driver, handle) = RaftDriver::new(node, transport, config);
		let driver_join = tokio::spawn(driver.run());

		eprintln!("  node {id}: listening on 127.0.0.1:{}", ports[i]);
		nodes.push(ClusterNode {
			id,
			handle,
			driver_join,
			_server_join: server_join,
		});
	}

	eprintln!("\n=== Waiting for leader election ===\n");

	let mut leader_id = None;
	for _ in 0..100 {
		tokio::time::sleep(Duration::from_millis(100)).await;
		for node in &nodes {
			let status = node.handle.status();
			if status.role == "leader" {
				leader_id = Some(node.id);
				eprintln!(
					"  node {}: LEADER at term {}, commit={}, applied={}",
					status.node_id, status.term, status.commit_index, status.applied_index
				);
			}
		}
		if leader_id.is_some() {
			break;
		}
	}

	let leader_id = match leader_id {
		Some(id) => id,
		None => {
			eprintln!("ERROR: no leader elected after 10 seconds");
			print_all_status(&nodes);
			std::process::exit(1);
		}
	};

	// Print all node statuses.
	print_all_status(&nodes);

	eprintln!("\n=== Proposing writes ===\n");

	let leader_handle = &nodes.iter().find(|n| n.id == leader_id).unwrap().handle;

	for i in 1..=3 {
		let key = format!("key{i}");
		let value = format!("value{i}");
		let cmd = test_write(&key, &value, i);
		match leader_handle.propose(cmd).await {
			Ok(index) => eprintln!("  put {key}={value} => committed at index {index}"),
			Err(e) => eprintln!("  put {key}={value} => ERROR: {e}"),
		}
	}

	// Wait for replication to propagate.
	tokio::time::sleep(Duration::from_millis(500)).await;

	eprintln!("\n=== Final cluster status ===\n");
	print_all_status(&nodes);

	eprintln!("\n=== Verifying state on all nodes ===\n");
	for node in &nodes {
		let status = node.handle.status();
		eprintln!(
			"  node {}: role={}, term={}, commit={}, applied={}",
			node.id, status.role, status.term, status.commit_index, status.applied_index
		);
	}

	eprintln!("\n=== Shutting down ===");
	for node in nodes {
		drop(node.handle);
		let _ = node.driver_join.await;
	}
	eprintln!("done.");
}

fn print_all_status(nodes: &[ClusterNode]) {
	for node in nodes {
		let s = node.handle.status();
		let leader_str = match s.leader {
			Some(id) => format!("(leader=n{id})"),
			None => String::new(),
		};
		eprintln!(
			"  node {}: {} {leader_str} term={} commit={} applied={}",
			s.node_id, s.role, s.term, s.commit_index, s.applied_index
		);
	}
}
