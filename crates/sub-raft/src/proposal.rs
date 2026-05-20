// Copyright (c) 2026 ReifyDB
// SPDX-License-Identifier: AGPL-3.0-or-later

use std::{
	error::Error,
	fmt::{Display, Formatter, Result as FmtResult},
	sync::mpsc::SyncSender,
};

use crate::{log::Index, message::Command, node::NodeId};

pub struct Proposal {
	pub command: Command,
	pub result_tx: SyncSender<Result<Index, ProposalError>>,
}

#[derive(Debug, Clone)]
pub enum ProposalError {
	NotLeader(Option<NodeId>),

	Shutdown,

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
