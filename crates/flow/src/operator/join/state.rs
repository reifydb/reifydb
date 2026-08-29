// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::operator::state::KeyspaceId;

use crate::operator::join::store::Store;

pub(crate) struct JoinState {
	pub(crate) left: Store,
	pub(crate) right: Store,
}

impl JoinState {
	pub(crate) fn new() -> Self {
		Self {
			left: Store::new(JoinSide::Left),
			right: Store::new(JoinSide::Right),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinSide {
	Left,
	Right,
}

impl JoinSide {
	pub(crate) fn keyspace(&self) -> KeyspaceId {
		match self {
			Self::Left => KeyspaceId::JOIN_LEFT,
			Self::Right => KeyspaceId::JOIN_RIGHT,
		}
	}

	pub(crate) fn tag(&self) -> u8 {
		match self {
			Self::Left => 0,
			Self::Right => 1,
		}
	}

	pub(crate) fn from_tag(tag: u8) -> Option<Self> {
		match tag {
			0 => Some(Self::Left),
			1 => Some(Self::Right),
			_ => None,
		}
	}
}
