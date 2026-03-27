// Copyright (c) 2025 ReifyDB
// SPDX-License-Identifier: Apache-2.0

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker

use reifydb_core::{common::CommitVersion, delta::Delta};

use crate::{
	log::{Entry, Index},
	node::{NodeId, Term},
};

/// A command stored in a Raft log entry.
#[derive(Clone, Debug, PartialEq)]
pub enum Command {
	/// A batch of deltas with leader-allocated version.
	Write {
		deltas: Vec<Delta>,
		version: CommitVersion,
	},
	/// No-op entry used during leader election to commit entries from prior terms.
	Noop,
}

/// A message envelope specifying sender and receiver.
#[derive(Clone, Debug, PartialEq)]
pub struct Envelope {
	/// The sender.
	pub from: NodeId,
	/// The sender's current term.
	pub term: Term,
	/// The recipient.
	pub to: NodeId,
	/// The message.
	pub message: Message,
}

/// A message sent between Raft nodes. Messages are sent asynchronously and may
/// be dropped or reordered.
#[derive(Clone, Debug, PartialEq)]
pub enum Message {
	/// Candidates campaign for leadership by soliciting votes from peers.
	Campaign {
		/// The index of the candidate's last log entry.
		last_index: Index,
		/// The term of the candidate's last log entry.
		last_term: Term,
	},

	/// Followers may vote for a single candidate per term.
	CampaignResponse {
		/// If true, the follower granted the candidate a vote.
		vote: bool,
	},

	/// Leaders send periodic heartbeats to prevent elections and advance commit
	/// indexes on followers.
	Heartbeat {
		/// The index of the leader's last log entry.
		last_index: Index,
		/// The index of the leader's last committed log entry.
		commit_index: Index,
	},

	/// Followers respond to leader heartbeats.
	HeartbeatResponse {
		/// If non-zero, the heartbeat's last_index which matched the follower's log.
		match_index: Index,
	},

	/// Leaders replicate log entries to followers.
	Append {
		/// The index of the log entry to append after.
		base_index: Index,
		/// The term of the base entry.
		base_term: Term,
		/// Log entries to append. Must start at base_index + 1.
		entries: Vec<Entry>,
	},

	/// Followers accept or reject appends from the leader.
	AppendResponse {
		/// If non-zero, the follower appended entries up to this index.
		match_index: Index,
		/// If non-zero, the follower rejected an append at this base index.
		reject_index: Index,
	},
}
