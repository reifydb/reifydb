// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crc32fast::Hasher;

use crate::log::{LogIndex, NodeId, Term, VoteSeq};

pub const SLOT_BYTES: usize = 40;

pub const SLOTS: usize = 2;

pub const FILE_BYTES: usize = SLOT_BYTES;

pub const MAGIC: u32 = u32::from_le_bytes(*b"RVOT");

pub const NONE: u64 = u64::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct State {
	pub term: Term,
	pub voted_for: Option<NodeId>,
	pub commit_index: LogIndex,
}

impl State {
	pub const EMPTY: Self = Self {
		term: Term::ZERO,
		voted_for: None,
		commit_index: LogIndex::ZERO,
	};

	pub fn encode(&self, seq: VoteSeq) -> [u8; SLOT_BYTES] {
		let mut out = [0u8; SLOT_BYTES];
		out[0..4].copy_from_slice(&MAGIC.to_le_bytes());
		out[8..16].copy_from_slice(&seq.as_u64().to_le_bytes());
		out[16..24].copy_from_slice(&self.term.as_u64().to_le_bytes());
		out[24..32].copy_from_slice(&voted_for_bits(self.voted_for).to_le_bytes());
		out[32..40].copy_from_slice(&self.commit_index.as_u64().to_le_bytes());
		let checksum = checksum(&out[8..]);
		out[4..8].copy_from_slice(&checksum.to_le_bytes());
		out
	}

	pub fn decode(buf: &[u8; SLOT_BYTES]) -> Option<(Self, VoteSeq)> {
		if u32::from_le_bytes(buf[0..4].try_into().unwrap()) != MAGIC {
			return None;
		}
		if u32::from_le_bytes(buf[4..8].try_into().unwrap()) != checksum(&buf[8..]) {
			return None;
		}
		let state = Self {
			term: Term::new(u64::from_le_bytes(buf[16..24].try_into().unwrap())),
			voted_for: voted_for_of(u64::from_le_bytes(buf[24..32].try_into().unwrap())),
			commit_index: LogIndex::new(u64::from_le_bytes(buf[32..40].try_into().unwrap())),
		};
		Some((state, VoteSeq::new(u64::from_le_bytes(buf[8..16].try_into().unwrap()))))
	}
}

fn voted_for_bits(voted_for: Option<NodeId>) -> u64 {
	match voted_for {
		Some(node) => node.as_u64(),
		None => NONE,
	}
}

fn voted_for_of(bits: u64) -> Option<NodeId> {
	if bits == NONE {
		None
	} else {
		Some(NodeId::new(bits))
	}
}

fn checksum(bytes: &[u8]) -> u32 {
	let mut hasher = Hasher::new();
	hasher.update(bytes);
	hasher.finalize()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_slot_lays_its_fields_out_at_the_documented_offsets() {
		// The offsets are the on disk format; moving one makes an existing vote unreadable,
		// and an unreadable vote is a node that cannot safely rejoin.
		let raw = State {
			term: Term::new(0x0102030405060708),
			voted_for: Some(NodeId::new(0x1112131415161718)),
			commit_index: LogIndex::new(0x2122232425262728),
		}
		.encode(VoteSeq::new(0x3132333435363738));

		assert_eq!(raw.len(), SLOT_BYTES);
		assert_eq!(&raw[0..4], &MAGIC.to_le_bytes());
		assert_eq!(&raw[8..16], &0x3132333435363738u64.to_le_bytes());
		assert_eq!(&raw[16..24], &0x0102030405060708u64.to_le_bytes());
		assert_eq!(&raw[24..32], &0x1112131415161718u64.to_le_bytes());
		assert_eq!(&raw[32..40], &0x2122232425262728u64.to_le_bytes());
	}

	#[test]
	fn no_vote_encodes_as_the_sentinel_and_decodes_back_to_none() {
		// Option is only sound if the sentinel survives the round trip; a node id decoded
		// out of the none slot would let a node that never voted claim it had.
		let raw = State::EMPTY.encode(VoteSeq::FIRST);

		assert_eq!(&raw[24..32], &NONE.to_le_bytes());
		assert_eq!(State::decode(&raw).unwrap().0.voted_for, None);
	}
}
