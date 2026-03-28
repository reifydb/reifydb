// Copyright (c) 2025 ReifyDB
// SPDX-License-Identifier: Apache-2.0

//! Per-node Raft REPL with a full ReifyDB engine.
//!
//! Each process is one node. Run 3 terminals for a 3-node cluster.
//! SQL/RQL on the leader replicates to all followers via Raft.
//!
//! Usage:
//!   cargo run -p raft -- --node-id 1 --bind 127.0.0.1:9100 \
//!     --peer 2=127.0.0.1:9200 --peer 3=127.0.0.1:9300

use std::{
	io::{BufRead, Write},
	net::SocketAddr,
	time::Duration,
};

use reifydb::{server, value::identity::IdentityId};
use reifydb_sub_raft::{
	config::{PeerConfig, RaftConfig},
	node::NodeId,
};
use reifydb_type::params::Params;

fn main() {
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

	eprintln!("node {node_id}: building engine...");

	let mut raft_config = RaftConfig::default()
		.node_id(node_id)
		.bind_addr(bind_addr)
		.heartbeat_interval(3)
		.election_timeout_range(8..15)
		.max_append_entries(100)
		.tick_interval(Duration::from_millis(50))
		.recv_interval(Duration::from_millis(5))
		.proposal_channel_capacity(64);

	for p in peers {
		raft_config = raft_config.peer(p.node_id, p.addr);
	}

	let mut db = server::memory().with_raft(raft_config).build().expect("failed to build database");

	eprintln!("node {node_id}: starting...");
	db.start().expect("failed to start database");

	let raft = db.sub_raft().expect("raft subsystem not found");
	let handle = raft.raft().expect("raft handle not available").clone();

	eprintln!("node {node_id}: waiting for leader election...");
	loop {
		std::thread::sleep(Duration::from_millis(100));
		let status = handle.status();
		if status.role == "leader" || status.leader.is_some() {
			eprintln!("node {node_id}: {} (term={}, leader={:?})", status.role, status.term, status.leader);
			break;
		}
	}

	eprintln!("node {node_id}: ready. Commands: admin <RQL>, command <RQL>, query <RQL>, status, quit\n");

	let stdin = std::io::stdin();
	let mut stdout = std::io::stdout();
	let identity = IdentityId::system();

	loop {
		let status = handle.status();
		let role_char = match status.role {
			"leader" => "L",
			"follower" => "F",
			"candidate" => "C",
			_ => "?",
		};
		print!("raft[{node_id}|{role_char}]> ");
		stdout.flush().unwrap();

		let mut line = String::new();
		if stdin.lock().read_line(&mut line).unwrap() == 0 {
			break;
		}
		let line = line.trim();
		if line.is_empty() {
			continue;
		}

		if line == "quit" || line == "exit" {
			break;
		}

		if line == "status" {
			let s = handle.status();
			let leader_str = s.leader.map(|id| format!("n{id}")).unwrap_or("none".into());
			println!(
				"node={} role={} term={} commit={} applied={} leader={}",
				s.node_id, s.role, s.term, s.commit_index, s.applied_index, leader_str
			);
			continue;
		}

		let (cmd, rql) = match line.split_once(' ') {
			Some((c, r)) => (c, r),
			None => {
				println!("usage: admin|command|query <RQL>");
				continue;
			}
		};

		let result = match cmd {
			"admin" => db.admin_as(identity.clone(), rql, Params::None),
			"command" => db.command_as(identity.clone(), rql, Params::None),
			"query" => db.query_as(identity.clone(), rql, Params::None),
			_ => {
				println!("unknown command '{cmd}'. Use: admin, command, query, status, quit");
				continue;
			}
		};

		match result {
			Ok(frames) => {
				if frames.is_empty() {
					println!("ok");
				} else {
					for frame in &frames {
						println!("{frame:?}");
					}
				}
			}
			Err(e) => println!("ERROR: {e:?}"),
		}
	}

	eprintln!("\nnode {node_id}: shutting down...");
	db.stop().expect("failed to stop database");
	eprintln!("done.");
}
