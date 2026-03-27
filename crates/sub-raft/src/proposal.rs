// Copyright (c) 2025 ReifyDB
// SPDX-License-Identifier: Apache-2.0

use tokio::sync::oneshot;

use crate::{log::Index, message::Command, node::NodeId};

/// A proposal submitted to the Raft driver by the write path.
pub struct Proposal {
	pub command: Command,
	pub result_tx: oneshot::Sender<Result<Index, ProposalError>>,
}

/// Errors that can occur when proposing a command.
#[derive(Debug, Clone)]
pub enum ProposalError {
	/// This node is not the leader. Contains the leader NodeId if known.
	NotLeader(Option<NodeId>),
	/// The driver has shut down.
	Shutdown,
}

impl std::fmt::Display for ProposalError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::NotLeader(Some(id)) => write!(f, "not leader, leader is node {id}"),
			Self::NotLeader(None) => write!(f, "not leader, leader unknown"),
			Self::Shutdown => write!(f, "driver shut down"),
		}
	}
}

impl std::error::Error for ProposalError {}
