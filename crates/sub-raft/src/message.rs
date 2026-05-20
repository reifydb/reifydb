// Copyright (c) 2026 ReifyDB
// SPDX-License-Identifier: AGPL-3.0-or-later

use reifydb_core::{common::CommitVersion, delta::Delta};
use serde::{Deserialize, Serialize};

use crate::{
	log::{Entry, Index},
	node::{NodeId, Term},
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Command {
	WriteMulti {
		deltas: Vec<Delta>,
		version: CommitVersion,
	},

	WriteSingle {
		deltas: Vec<Delta>,
	},

	Noop,
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
