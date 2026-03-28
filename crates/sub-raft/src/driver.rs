// Copyright (c) 2025 ReifyDB
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, sync::mpsc::SyncSender, time::Duration};

use tokio::{
	select,
	sync::{mpsc, watch},
	time::interval,
};

use crate::{
	log::Index,
	message::Command,
	node::{Node, NodeId, Term},
	proposal::{Proposal, ProposalError},
	transport::Transport,
};

/// Observable snapshot of the node's state.
#[derive(Clone, Debug)]
pub struct NodeStatus {
	pub node_id: NodeId,
	pub role: &'static str,
	pub term: Term,
	pub commit_index: Index,
	pub applied_index: Index,
	pub last_index: Index,
	pub leader: Option<NodeId>,
}

/// Configuration for the Raft driver loop.
pub struct DriverConfig {
	/// Interval between logical ticks.
	pub tick_interval: Duration,
	/// Interval between transport receive polls.
	pub recv_interval: Duration,
	/// Capacity of the proposal channel.
	pub proposal_channel_capacity: usize,
}

impl Default for DriverConfig {
	fn default() -> Self {
		Self {
			tick_interval: Duration::from_millis(100),
			recv_interval: Duration::from_millis(10),
			proposal_channel_capacity: 256,
		}
	}
}

/// Cheap, cloneable handle for submitting proposals to the driver.
#[derive(Clone)]
pub struct Raft {
	proposal_tx: mpsc::Sender<Proposal>,
	status_rx: watch::Receiver<NodeStatus>,
}

impl Raft {
	/// Propose a command and wait for it to be committed.
	/// Returns the log index of the committed entry.
	///
	/// This is a synchronous call safe to use from any thread (Rayon, OS, etc.).
	/// It does not require a tokio runtime on the calling thread.
	pub fn propose(&self, command: Command) -> Result<Index, ProposalError> {
		let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
		self.proposal_tx
			.try_send(Proposal {
				command,
				result_tx,
			})
			.map_err(|e| match e {
				mpsc::error::TrySendError::Full(_) => ProposalError::Overloaded,
				mpsc::error::TrySendError::Closed(_) => ProposalError::Shutdown,
			})?;
		result_rx.recv().map_err(|_| ProposalError::Shutdown)?
	}

	/// Returns true if the driver has shut down.
	pub fn is_closed(&self) -> bool {
		self.proposal_tx.is_closed()
	}

	/// Returns the latest observed node status.
	pub fn status(&self) -> NodeStatus {
		self.status_rx.borrow().clone()
	}

	/// Waits until the node status changes.
	pub async fn status_changed(&mut self) -> NodeStatus {
		let _ = self.status_rx.changed().await;
		self.status_rx.borrow().clone()
	}
}

/// Drives a Raft node: receives messages via transport, ticks on an
/// interval, accepts proposals, and sends outbound messages.
pub struct RaftDriver<T: Transport> {
	node: Option<Node>,
	transport: T,
	proposal_rx: mpsc::Receiver<Proposal>,
	pending_proposals: HashMap<Index, SyncSender<Result<Index, ProposalError>>>,
	status_tx: watch::Sender<NodeStatus>,
	config: DriverConfig,
}

impl<T: Transport> RaftDriver<T> {
	/// Creates a new driver and its handle.
	pub fn new(node: Node, transport: T, config: DriverConfig) -> (Self, Raft) {
		let (proposal_tx, proposal_rx) = mpsc::channel(config.proposal_channel_capacity);
		let initial_status = Self::snapshot_status(&node);
		let (status_tx, status_rx) = watch::channel(initial_status);
		let driver = Self {
			node: Some(node),
			transport,
			proposal_rx,
			pending_proposals: HashMap::new(),
			status_tx,
			config,
		};
		let handle = Raft {
			proposal_tx,
			status_rx,
		};
		(driver, handle)
	}

	fn snapshot_status(node: &Node) -> NodeStatus {
		NodeStatus {
			node_id: node.id(),
			role: node.role(),
			term: node.term(),
			commit_index: node.get_commit_index().0,
			applied_index: node.applied_index(),
			last_index: node.log().get_last_index().0,
			leader: node.leader(),
		}
	}

	fn publish_status(&self) {
		if let Some(node) = &self.node {
			let _ = self.status_tx.send(Self::snapshot_status(node));
		}
	}

	/// Run the driver loop. This blocks until the proposal channel is closed
	/// (all Raft instances dropped) or an unrecoverable error occurs.
	pub async fn run(mut self) {
		let mut tick_interval = interval(self.config.tick_interval);
		let mut recv_interval = interval(self.config.recv_interval);

		loop {
			select! {
				_ = tick_interval.tick() => {
					self.do_tick();
				}
				result = self.proposal_rx.recv() => {
					match result {
						Some(proposal) => self.do_propose(proposal),
						None => break, // all handles dropped
					}
				}
				_ = recv_interval.tick() => {
					self.do_receive();
				}
			}
		}

		self.fail_all_pending(ProposalError::Shutdown);
	}

	fn do_tick(&mut self) {
		let node = self.node.take().expect("node missing");
		let old_commit = node.get_commit_index().0;
		let mut node = node.tick();
		self.flush_outbox(&mut node);
		self.complete_proposals(&node, old_commit);
		self.check_leadership(&node);
		self.node = Some(node);
		self.publish_status();
	}

	fn do_receive(&mut self) {
		let messages = self.transport.receive();
		if messages.is_empty() {
			return;
		}

		for msg in messages {
			let node = self.node.take().expect("node missing");
			if msg.to != node.id() {
				self.node = Some(node);
				continue;
			}
			let old_commit = node.get_commit_index().0;
			let mut node = node.step(msg);
			self.flush_outbox(&mut node);
			self.complete_proposals(&node, old_commit);
			self.check_leadership(&node);
			self.node = Some(node);
		}
		self.publish_status();
	}

	fn do_propose(&mut self, proposal: Proposal) {
		let node = self.node.as_mut().expect("node missing");

		if node.role() != "leader" {
			let leader = node.leader();
			let _ = proposal.result_tx.send(Err(ProposalError::NotLeader(leader)));
			return;
		}

		let index = node.propose(proposal.command);
		self.pending_proposals.insert(index, proposal.result_tx);

		let node = self.node.as_mut().expect("node missing");
		for msg in node.drain_outbox() {
			self.transport.send(msg);
		}
	}

	fn flush_outbox(&self, node: &mut Node) {
		for msg in node.drain_outbox() {
			self.transport.send(msg);
		}
	}

	fn complete_proposals(&mut self, node: &Node, old_commit: Index) {
		let new_commit = node.get_commit_index().0;
		if new_commit <= old_commit {
			return;
		}

		for index in (old_commit + 1)..=new_commit {
			if let Some(tx) = self.pending_proposals.remove(&index) {
				let _ = tx.send(Ok(index));
			}
		}
	}

	fn check_leadership(&mut self, node: &Node) {
		if node.role() != "leader" && !self.pending_proposals.is_empty() {
			self.fail_all_pending(ProposalError::NotLeader(node.leader()));
		}
	}

	fn fail_all_pending(&mut self, error: ProposalError) {
		for (_, tx) in self.pending_proposals.drain() {
			let _ = tx.send(Err(error.clone()));
		}
	}
}
