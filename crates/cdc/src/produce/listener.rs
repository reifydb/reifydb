// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crossbeam_channel::Sender;
use reifydb_core::event::{EventListener, transaction::PostCommitEvent};
use reifydb_runtime::clock::Clock;

use super::worker::CdcWorkItem;

pub struct CdcEventListener {
	sender: Sender<CdcWorkItem>,
	clock: Clock,
}

impl CdcEventListener {
	pub fn new(sender: Sender<CdcWorkItem>, clock: Clock) -> Self {
		Self {
			sender,
			clock,
		}
	}
}

impl EventListener<PostCommitEvent> for CdcEventListener {
	fn on(&self, event: &PostCommitEvent) {
		let item = CdcWorkItem {
			version: *event.version(),
			timestamp: self.clock.now_millis(),
			deltas: event.deltas().iter().cloned().collect(),
		};

		let _ = self.sender.send(item);
	}
}

#[cfg(test)]
pub mod tests {
	use crossbeam_channel::unbounded;
	use reifydb_core::{
		common::CommitVersion,
		delta::Delta,
		encoded::{encoded::EncodedValues, key::EncodedKey},
	};
	use reifydb_runtime::clock::MockClock;
	use reifydb_type::util::cowvec::CowVec;

	use super::*;

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey(CowVec::new(s.as_bytes().to_vec()))
	}

	fn make_values(s: &str) -> EncodedValues {
		EncodedValues(CowVec::new(s.as_bytes().to_vec()))
	}

	#[test]
	fn test_listener_forwards_event() {
		let (sender, receiver) = unbounded();
		let clock = Clock::Mock(MockClock::from_millis(1000));
		let listener = CdcEventListener::new(sender, clock);

		let deltas = CowVec::new(vec![
			Delta::Set {
				key: make_key("key1"),
				values: make_values("value1"),
			},
			Delta::Remove {
				key: make_key("key2"),
			},
		]);

		let event = PostCommitEvent::new(deltas, CommitVersion(42));

		listener.on(&event);

		let item = receiver.try_recv().unwrap();
		assert_eq!(item.version, CommitVersion(42));
		assert_eq!(item.deltas.len(), 2);
		assert_eq!(item.timestamp, 1000);
	}
}
