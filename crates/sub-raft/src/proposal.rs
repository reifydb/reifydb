// Copyright (c) 2025 ReifyDB
// SPDX-License-Identifier: Apache-2.0

use std::{
	error::Error,
	fmt::{Display, Formatter, Result as FmtResult},
	sync::mpsc::SyncSender,
};

use crate::{log::Index, message::Command, node::NodeId};

/// A proposal submitted to the Raft driver by the write path.
pub struct Proposal {
	pub command: Command,
	pub result_tx: SyncSender<Result<Index, ProposalError>>,
}

/// Errors that can occur when proposing a command.
#[derive(Debug, Clone)]
pub enum ProposalError {
	/// This node is not the leader. Contains the leader NodeId if known.
	NotLeader(Option<NodeId>),
	/// The driver has shut down.
	Shutdown,
	/// The proposal channel is full (backpressure).
	Overloaded,
}

impl Display for ProposalError {
	fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
		match self {
			Self::NotLeader(Some(id)) => write!(f, "not leader, leader is node {id}"),
			Self::NotLeader(None) => write!(f, "not leader, leader unknown"),
			Self::Shutdown => write!(f, "driver shut down"),
			Self::Overloaded => write!(f, "proposal channel full"),
		}
	}
}

impl Error for ProposalError {}
