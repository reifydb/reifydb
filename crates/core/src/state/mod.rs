// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Operator state: the [`timer`] contract every operator reads and writes through, and the group/horizon/keyspace
//! vocabulary reclamation works in. Not window-specific: extern_c, extern_rust, distinct and take all route their
//! state through the same contract.

pub mod timer;

pub mod group {
	use reifydb_macro::operator_state;

	use crate::metrics::heap::HeapSize;

	#[operator_state]
	#[derive(Debug, Clone, Default, PartialEq, Eq)]
	pub struct GroupRecord {
		pub group: Vec<u8>,
		pub keyspace: u8,
	}

	impl GroupRecord {
		pub fn new(group: impl Into<Vec<u8>>, keyspace: u8) -> Self {
			Self {
				group: group.into(),
				keyspace,
			}
		}
	}

	impl HeapSize for GroupRecord {
		fn heap_size(&self) -> usize {
			self.group.capacity()
		}
	}
}

pub mod horizon {
	use reifydb_value::value::datetime::DateTime;

	#[derive(Debug, Clone, Copy, PartialEq, Eq)]
	pub struct Cutoff(pub DateTime);

	impl Cutoff {
		pub fn instant(&self) -> DateTime {
			self.0
		}

		pub fn raw(&self) -> u64 {
			self.0.to_nanos()
		}
	}
}
