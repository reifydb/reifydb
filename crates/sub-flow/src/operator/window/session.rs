// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use postcard::{from_bytes, to_stdvec};
use reifydb_core::{
	common::WindowKind, encoded::key::EncodedKey, internal, util::encoding::keycode::serializer::KeySerializer,
};
use reifydb_runtime::hash::Hash128;
use reifydb_value::{Result, error::Error, value::blob::Blob};

use super::operator::WindowOperator;
use crate::{operator::stateful::window::WindowStateful, transaction::FlowTransaction};

impl WindowOperator {
	pub(super) fn session_gap_ms(&self) -> u64 {
		match &self.kind {
			WindowKind::Session {
				gap,
				..
			} => gap.milliseconds().unwrap_or(0) as u64,
			_ => 0,
		}
	}

	fn create_session_tracker_key(&self, group_hash: Hash128) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(32);
		serializer.extend_bytes(b"ses:");
		serializer.extend_u128(group_hash);
		serializer.finish()
	}

	pub(super) fn load_session_tracker(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
	) -> Result<(u64, u64)> {
		let tracker_key = self.create_session_tracker_key(group_hash);
		let state_row = self.load_state(txn, &tracker_key)?;

		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok((0, 0));
		}

		let blob = self.layout.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok((0, 0));
		}

		let tracker: (u64, u64) = from_bytes(blob.as_ref()).unwrap_or((0, 0));
		Ok(tracker)
	}

	pub(super) fn save_session_tracker(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
		session_id: u64,
		last_event_time: u64,
	) -> Result<()> {
		let tracker_key = self.create_session_tracker_key(group_hash);
		let serialized = to_stdvec(&(session_id, last_event_time))
			.map_err(|e| Error(Box::new(internal!("Failed to serialize session tracker: {}", e))))?;
		let mut state_row = self.layout.allocate();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut state_row, 0, &blob);
		self.save_state(txn, &tracker_key, state_row)
	}
}
