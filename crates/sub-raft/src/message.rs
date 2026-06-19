// Copyright (c) 2026 ReifyDB
// SPDX-License-Identifier: Apache-2.0

use reifydb_core::{common::CommitVersion, delta::Delta, interface::change::Change};
use serde::{Deserialize, Serialize};

use crate::{
	log::{Entry, Index},
	node::{NodeId, Term},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command {
	WriteMulti {
		deltas: Vec<Delta>,
		version: CommitVersion,
		changes: Vec<Change>,
	},

	WriteSingle {
		deltas: Vec<Delta>,
	},

	Noop,
}

impl PartialEq for Command {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(
				Command::WriteMulti {
					deltas: a_deltas,
					version: a_version,
					..
				},
				Command::WriteMulti {
					deltas: b_deltas,
					version: b_version,
					..
				},
			) => a_deltas == b_deltas && a_version == b_version,
			(
				Command::WriteSingle {
					deltas: a,
				},
				Command::WriteSingle {
					deltas: b,
				},
			) => a == b,
			(Command::Noop, Command::Noop) => true,
			_ => false,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
	pub from: NodeId,

	pub term: Term,

	pub to: NodeId,

	pub message: Message,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Message {
	Campaign {
		last_index: Index,

		last_term: Term,
	},

	CampaignResponse {
		vote: bool,
	},

	Heartbeat {
		last_index: Index,

		commit_index: Index,
	},

	HeartbeatResponse {
		match_index: Index,
	},

	Append {
		base_index: Index,

		base_term: Term,

		entries: Vec<Entry>,
	},

	AppendResponse {
		match_index: Index,

		reject_index: Index,
	},
}
